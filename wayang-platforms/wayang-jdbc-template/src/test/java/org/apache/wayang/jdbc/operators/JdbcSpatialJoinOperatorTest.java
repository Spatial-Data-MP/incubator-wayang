package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.JdbcExecutor;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcSpatialJoinOperatorTest extends OperatorTestBase {

    /**
     * Minimal concrete test subclass to allow returning the HSQLDB platform.
     */
    private static class TestJdbcSpatialJoinOperator extends JdbcSpatialJoinOperator {

        private final Platform platform;

        @SuppressWarnings("unchecked")
        public TestJdbcSpatialJoinOperator(TransformationDescriptor keyDescriptor0,
                                           TransformationDescriptor keyDescriptor1,
                                           SpatialPredicate predicate,
                                           Platform platform) {
            super(keyDescriptor0, keyDescriptor1, predicate);
            this.platform = platform;
        }

        @Override
        public JdbcPlatformTemplate getPlatform() {
            return (JdbcPlatformTemplate) this.platform;
        }
    }

    @Test
    void testSpatialJoinWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();
        ExecutionStage sqlStage = mock(ExecutionStage.class);

        // Create two test tables with a "geom" column containing WKT.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testA (id INT, geom VARCHAR(255));");
            statement.execute("INSERT INTO testA VALUES (0, 'POINT (0 0)');");
            statement.execute("CREATE TABLE testB (id INT, geom VARCHAR(255));");
            statement.execute("INSERT INTO testB VALUES (0, 'POINT (0 0)');");
        }

        JdbcTableSource tableSourceA = new HsqldbTableSource("testA");
        JdbcTableSource tableSourceB = new HsqldbTableSource("testB");

        ExecutionTask tableSourceATask = new ExecutionTask(tableSourceA);
        tableSourceATask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceA.getOutput(0)));
        tableSourceATask.setStage(sqlStage);

        ExecutionTask tableSourceBTask = new ExecutionTask(tableSourceB);
        tableSourceBTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceB.getOutput(0)));
        tableSourceBTask.setStage(sqlStage);

        // Key descriptors: parse WKT from the second field for runtime, and provide SQL mapping to table/column.
        TransformationDescriptor<Record, WGeometry> leftKey =
                new TransformationDescriptor<>(
                        (record) -> WGeometry.fromStringInput((String) record.getField(1)),
                        Record.class,
                        WGeometry.class
                ).withSqlImplementation("testA", "geom");

        TransformationDescriptor<Record, WGeometry> rightKey =
                new TransformationDescriptor<>(
                        (record) -> WGeometry.fromStringInput((String) record.getField(1)),
                        Record.class,
                        WGeometry.class
                ).withSqlImplementation("testB", "geom");

        final ExecutionOperator joinOperator = new TestJdbcSpatialJoinOperator(
                leftKey,
                rightKey,
                SpatialPredicate.INTERSECTS,
                HsqldbPlatform.getInstance()
        );

        ExecutionTask joinTask = new ExecutionTask(joinOperator);
        tableSourceATask.getOutputChannel(0).addConsumer(joinTask, 0);
        tableSourceBTask.getOutputChannel(0).addConsumer(joinTask, 1);
        joinTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, joinOperator.getOutput(0)));
        joinTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceATask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(joinTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        joinTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));

        String sql = sqlQueryChannelInstance.getSqlQuery();

        System.out.println(sql);

        assertEquals(
                "SELECT * FROM testA JOIN testB ON ST_Intersects(testA.geom, testB.geom);",
                sql
        );
    }
}