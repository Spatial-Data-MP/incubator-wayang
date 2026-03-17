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

import org.apache.wayang.basic.operators.CountOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

/**
 * JDBC-based {@link CountOperator} that pushes {@code SELECT COUNT(*)} down to SQL.
 * Outputs via {@link org.apache.wayang.jdbc.channels.SqlCountQueryChannel} so the
 * optimizer uses a dedicated conversion path.
 */
public abstract class JdbcCountOperator<Type> extends CountOperator<Type> implements JdbcExecutionOperator {

    public JdbcCountOperator(DataSetType<Type> type) {
        super(type);
    }

    public JdbcCountOperator(CountOperator<Type> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return "COUNT(*)";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSqlCountQueryChannelDescriptor());
    }
}
