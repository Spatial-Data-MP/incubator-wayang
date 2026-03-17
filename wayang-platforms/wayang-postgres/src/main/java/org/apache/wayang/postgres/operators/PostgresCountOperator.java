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

package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.operators.CountOperator;
import org.apache.wayang.jdbc.operators.JdbcCountOperator;

/**
 * PostgreSQL implementation of the {@link JdbcCountOperator}.
 * Pushes {@code SELECT COUNT(*)} down to PostgreSQL.
 */
public class PostgresCountOperator<Type> extends JdbcCountOperator<Type> implements PostgresExecutionOperator {

    public PostgresCountOperator(CountOperator<Type> that) {
        super(that);
    }

    @Override
    protected PostgresCountOperator<Type> createCopy() {
        return new PostgresCountOperator<>(this);
    }
}
