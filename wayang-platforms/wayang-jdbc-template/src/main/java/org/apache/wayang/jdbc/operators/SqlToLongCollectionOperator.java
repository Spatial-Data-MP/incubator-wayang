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

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This {@link Operator} converts a {@link org.apache.wayang.jdbc.channels.SqlCountQueryChannel}
 * to a {@link CollectionChannel} containing a single {@link Long} value.
 */
public class SqlToLongCollectionOperator extends UnaryToUnaryOperator<Long, Long> implements JavaExecutionOperator {

    private final JdbcPlatformTemplate jdbcPlatform;

    public SqlToLongCollectionOperator(JdbcPlatformTemplate jdbcPlatform) {
        super(DataSetType.createDefault(Long.class), DataSetType.createDefault(Long.class), false);
        this.jdbcPlatform = jdbcPlatform;
    }

    protected SqlToLongCollectionOperator(SqlToLongCollectionOperator that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor executor,
            OptimizationContext.OperatorContext operatorContext) {

        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(executor.getConfiguration())
                .createJdbcConnection();

        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(input.getSqlQuery())) {
            long count = 0;
            if (rs.next()) {
                count = rs.getLong(1);
            }
            output.accept(Collections.singleton(count));
        } catch (SQLException e) {
            throw new WayangException("Could not execute SQL count query.", e);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }

        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("wayang.%s.sqltolongcollection.load.query", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()
        ));
        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("wayang.%s.sqltolongcollection.load.output", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()
        ));
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlCountQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList(
                String.format("wayang.%s.sqltolongcollection.load.query", this.jdbcPlatform.getPlatformId()),
                String.format("wayang.%s.sqltolongcollection.load.output", this.jdbcPlatform.getPlatformId())
        );
    }
}
