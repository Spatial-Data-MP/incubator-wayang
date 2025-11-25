/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.SpatialRelation;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.JdbcExecutor;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Plan-level test for a JDBC spatial filter operator, analogous to {@link JdbcJoinOperatorTest}.
 * It verifies that a spatial filter produces the expected SQL predicate in the generated query.
 */
class JdbcSpatialFilterOperatorTest extends OperatorTestBase {

    /**
     * Minimal concrete subclass that lets us set the SQL geometry column name and platform.
     * This reuses the generic {@link JdbcSpatialFilterOperator} logic.
     */
    private static class TestJdbcSpatialFilterOperator extends JdbcSpatialFilterOperator {

        private final Platform platform;

        TestJdbcSpatialFilterOperator(SpatialRelation relation,
                                      int columnIndex,
                                      WGeometry geometry,
                                      String geometryColumnSqlName,
                                      Platform platform) {
            super(relation, columnIndex, geometry);
            this.geometryColumnSqlName = geometryColumnSqlName;
            this.platform = platform;
        }

        @Override
        public JdbcPlatformTemplate getPlatform() {
            return (JdbcPlatformTemplate) this.platform;
        }
    }

    @Test
    void testSpatialFilterWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor())
                .thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();
        SqlQueryChannel.Descriptor sqlChannelDescriptor =
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        // Create a simple test table with a "geom" column.
        try (Connection jdbcConnection =
                     hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testGeom (id INT, geom VARCHAR(255));");
            statement.execute("INSERT INTO testGeom VALUES (0, 'POINT (0 0)');");
        }

        // Table source for testGeom.
        JdbcTableSource tableSource = new HsqldbTableSource("testGeom");

        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(
                0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0))
        );
        tableSourceTask.setStage(sqlStage);

        // Reference geometry for the spatial filter.
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry queryGeometry = geometryFactory.createPoint(new Coordinate(0.0, 0.0));
        WGeometry wGeometry = WGeometry.fromGeometry(queryGeometry);

        // Spatial filter: INTERSECTS on column "geom".
        ExecutionOperator spatialFilterOperator =
                new TestJdbcSpatialFilterOperator(
                        SpatialRelation.INTERSECTS,
                        1,
                        wGeometry,
                        "geom",
                        HsqldbPlatform.getInstance()
                );

        ExecutionTask spatialFilterTask = new ExecutionTask(spatialFilterOperator);
        // Wire table source → spatial filter.
        tableSourceTask.getOutputChannel(0).addConsumer(spatialFilterTask, 0);
        spatialFilterTask.setOutputChannel(
                0,
                new SqlQueryChannel(sqlChannelDescriptor, spatialFilterOperator.getOutput(0))
        );
        spatialFilterTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(spatialFilterTask));

        // Next stage that consumes the SQL via SqlToStreamOperator.
        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        spatialFilterTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        // Execute only the SQL stage: this will build the SQL string but not actually execute it against the DB.
        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor()
                        .getChannelInstance(sqlToStreamTask.getInputChannel(0));

        String sql = sqlQueryChannelInstance.getSqlQuery();
        String expectedPredicate =
                String.format("ST_Intersects(geom, ST_GeomFromText('%s', 4326))", wGeometry.getWKT());

        // We don’t assume the *exact* full query shape, but we do assert the important bits.
        assertTrue(sql.startsWith("SELECT"),
                "SQL should be a SELECT statement, but was: " + sql);
        assertTrue(sql.contains("FROM testGeom"),
                "SQL should select from testGeom, but was: " + sql);
        assertTrue(sql.contains(expectedPredicate),
                "SQL should contain spatial predicate: " + expectedPredicate + " but was: " + sql);
    }
}
