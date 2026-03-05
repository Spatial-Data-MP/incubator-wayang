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

package org.apache.wayang.jdbc.execution;

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutor extends ExecutorTemplate {

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutor(final JdbcPlatformTemplate platform, final Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    @Override
    public void execute(final ExecutionStage stage, final OptimizationContext optimizationContext, final ExecutionState executionState) {
        final Tuple2<String, SqlQueryChannel.Instance> pair = JdbcExecutor.createSqlQuery(stage, optimizationContext, this);
        final String query = pair.field0;
        final SqlQueryChannel.Instance queryChannel = pair.field1;

        queryChannel.setSqlQuery(query);

        // Return the tipChannelInstance.
        executionState.register(queryChannel);
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an {@link ExecutionTask}.
     */
    private static SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
            final OptimizationContext optimizationContext, final JdbcExecutor jdbcExecutor) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SqlQueryChannel : String.format("Illegal task: %s.", task);

        final SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext
                .getOperatorContext(task.getOperator());
        return outputChannel.createInstance(jdbcExecutor, operatorContext, 0);
    }

    /**
     * Holds the operators discovered during a backward walk of the execution stage.
     */
    private static final class StageOperators {
        final List<JdbcTableSource> tableSources = new ArrayList<>();
        final Collection<JdbcExecutionOperator> filters = new ArrayList<>(4);
        final Collection<JdbcExecutionOperator> joins = new ArrayList<>();
        JdbcProjectionOperator projection;
    }

    /**
     * Creates a query channel and the sql statement.
     */
    protected static Tuple2<String, SqlQueryChannel.Instance> createSqlQuery(final ExecutionStage stage,
            final OptimizationContext context, final JdbcExecutor jdbcExecutor) {
        final Collection<?> termTasks = stage.getTerminalTasks();
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        final ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];

        // Single backward walk: collects operators by type and builds channel lineage.
        final StageOperators ops = new StageOperators();
        final SqlQueryChannel.Instance tipChannelInstance = walkBackwards(
                termTask, stage, ops, new HashMap<>(), context, jdbcExecutor);

        assert !ops.tableSources.isEmpty() : "Invalid JDBC stage: no TableSource found";

        final StringBuilder query = createSqlString(
                jdbcExecutor, ops.tableSources.get(0), ops.filters, ops.projection, ops.joins);
        return new Tuple2<>(query.toString(), tipChannelInstance);
    }

    /**
     * Walks backwards from the given task through input channels, collecting operators
     * by type in forward traversal order and building channel lineage in a single pass.
     *
     * @return the {@link SqlQueryChannel.Instance} for the given task
     */
    private static SqlQueryChannel.Instance walkBackwards(final ExecutionTask task, final ExecutionStage stage,
            final StageOperators ops,
            final Map<ExecutionTask, SqlQueryChannel.Instance> visited,
            final OptimizationContext context, final JdbcExecutor jdbcExecutor) {
        final SqlQueryChannel.Instance cached = visited.get(task);
        if (cached != null) return cached;

        // Recurse into predecessors first (preserves forward traversal order).
        // Keep the first predecessor's channel instance for lineage linking.
        SqlQueryChannel.Instance predecessorInstance = null;
        for (int i = 0; i < task.getNumInputChannels(); i++) {
            final Channel inputChannel = task.getInputChannel(i);
            if (inputChannel == null) continue;
            final ExecutionTask producer = inputChannel.getProducer();
            if (producer != null && producer.getStage() == stage) {
                final SqlQueryChannel.Instance pi = walkBackwards(producer, stage, ops, visited, context, jdbcExecutor);
                if (predecessorInstance == null) predecessorInstance = pi;
            }
        }

        // Create channel instance, linking lineage to predecessor when available.
        final SqlQueryChannel.Instance channelInstance = instantiateOutboundChannel(task, context, jdbcExecutor);
        if (predecessorInstance != null) {
            channelInstance.getLineage().addPredecessor(predecessorInstance.getLineage());
        }
        visited.put(task, channelInstance);

        // Classify the current task's operator.
        final var operator = task.getOperator();
        if (operator instanceof JdbcTableSource) {
            ops.tableSources.add((JdbcTableSource) operator);
        } else if (operator instanceof JdbcFilterOperator || operator instanceof SpatialFilterOperator) {
            ops.filters.add((JdbcExecutionOperator) operator);
        } else if (operator instanceof JdbcProjectionOperator) {
            assert ops.projection == null : "Only one projection operator per stage is supported";
            ops.projection = (JdbcProjectionOperator) operator;
        } else if (operator instanceof JdbcJoinOperator || operator instanceof SpatialJoinOperator) {
            ops.joins.add((JdbcExecutionOperator) operator);
        } else if (!(operator instanceof TableSource)) {
            throw new WayangException(String.format("Unsupported JDBC execution task %s", task));
        }

        return channelInstance;
    }

    public static StringBuilder createSqlString(final JdbcExecutor jdbcExecutor, final JdbcTableSource tableOp,
            final Collection<JdbcExecutionOperator> filterTasks,
            JdbcProjectionOperator projectionTask,
            final Collection<JdbcExecutionOperator> joinTasks) {
        final String tableName = tableOp.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);
        final Collection<String> conditions = filterTasks.stream()
                .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler))
                .collect(Collectors.toList());
        final String projection = projectionTask == null ? "*" : projectionTask.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);
        final Collection<String> joins = joinTasks.stream()
                .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler))
                .collect(Collectors.toList());

        final StringBuilder sb = new StringBuilder(1000);
        sb.append("SELECT ").append(projection).append(" FROM ").append(tableName);
        if (!joins.isEmpty()) {
            final String separator = " ";
            for (final String join : joins) {
                sb.append(separator).append(join);
            }
        }
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (final String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }
        sb.append(';');
        return sb;
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (final SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private void saveResult(final FileChannel.Instance outputFileChannelInstance, final ResultSet rs)
            throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(
                outFs.create(outputFileChannelInstance.getSinglePath()))) {
            while (rs.next()) {
                // System.out.println(rs.getInt(1) + " " + rs.getString(2));
                final ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()) {
                    writer.write('\n');
                }
            }
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
