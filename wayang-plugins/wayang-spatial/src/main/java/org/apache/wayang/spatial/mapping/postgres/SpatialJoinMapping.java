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

package org.apache.wayang.spatial.mapping.postgres;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.postgres.platform.PostgresPlatform;
import org.apache.wayang.spatial.operators.postgres.PostgresSpatialJoinOperator;

import java.util.Collection;
import java.util.Collections;


/**
 * Mapping from {@link SpatialJoinOperator} to {@link PostgresSpatialJoinOperator}.
 */
@SuppressWarnings("unchecked")
public class SpatialJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<SpatialJoinOperator<Record, Record>> operatorPattern = new OperatorPattern<>(
                "spatialJoin",
                new SpatialJoinOperator<Record, Record>(
                        null,
                        null,
                        DataSetType.createDefault(Record.class),
                        DataSetType.createDefault(Record.class),
                        null
                ),
                false
        )
            .withAdditionalTest(op -> op.getKeyDescriptor0() instanceof TransformationDescriptor)
            .withAdditionalTest(op -> op.getKeyDescriptor1() instanceof TransformationDescriptor)
            .withAdditionalTest(op -> op.getKeyDescriptor0().getSqlImplementation() != null)
            .withAdditionalTest(op -> op.getKeyDescriptor1().getSqlImplementation() != null);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SpatialJoinOperator<Record, Record>>(
                (matchedOperator, epoch) -> {
                    return new PostgresSpatialJoinOperator<>(matchedOperator).at(epoch);
                }
        );
    }
}
