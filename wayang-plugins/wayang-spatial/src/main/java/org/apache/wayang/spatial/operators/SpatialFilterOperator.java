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

package org.apache.wayang.spatial.operators;

import org.apache.wayang.spatial.data.WGeometry;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.spatial.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;


/**
 * This operator returns a new dataset after filtering by applying predicateDescriptor.
 */
public class SpatialFilterOperator<Type> extends UnaryToUnaryOperator<Type, Type> {


    protected final SpatialPredicate relation;
    protected final TransformationDescriptor<Type, WGeometry> keyDescriptor;
    protected final WGeometry referenceGeometry;

    /**
     * Creates a new instance.SpatialRelation relation,
     *                                  FunctionDescriptor.SerializableFunction<Type, WGeometry> keyExtractor,
     *                                  Class<Type> inputClass,
     *                                  WGeometry geometry,
     *                                  String geometryColumnSqlName
     */
    public SpatialFilterOperator(SpatialPredicate relation,
                                 FunctionDescriptor.SerializableFunction<Type, WGeometry> keyExtractor,
                                 DataSetType<Type> inputClassDatasetType,
//                                 Class<Type> inputClass,
                                 WGeometry geometry) {
        super(inputClassDatasetType,
                inputClassDatasetType, true);
        // TODO: Find out what broadcast means
        this.relation = relation;
        this.keyDescriptor = new TransformationDescriptor<>(keyExtractor, inputClassDatasetType.getDataUnitType().getTypeClass(), WGeometry.class);
        this.referenceGeometry = geometry;
    }

    public TransformationDescriptor<Type, WGeometry> getKeyDescriptor() {
        return this.keyDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SpatialFilterOperator(SpatialFilterOperator<Type> that) {
        super(that);
        this.relation = that.relation;
        this.keyDescriptor = that.keyDescriptor;
        this.referenceGeometry = that.referenceGeometry;
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link SpatialFilterOperator}s.
     */
    private class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        /**
         * The expected selectivity to be applied in this instance.
         */
//        private final ProbabilisticDoubleInterval selectivity;
//
//        public CardinalityEstimator(PredicateDescriptor<?> predicateDescriptor, Configuration configuration) {
//            this.selectivity = configuration.getUdfSelectivityProvider().provideFor(predicateDescriptor);
//        }

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
//            Validate.isTrue(inputEstimates.length == SpatialFilterOperator.this.getNumInputs());
//            final CardinalityEstimate inputEstimate = inputEstimates[0];

//            return new CardinalityEstimate(
//                    (long) (inputEstimate.getLowerEstimate() * this.selectivity.getLowerEstimate()),
//                    (long) (inputEstimate.getUpperEstimate() * this.selectivity.getUpperEstimate()),
//                    inputEstimate.getCorrectnessProbability() * this.selectivity.getCorrectnessProbability()
//            );
            return new CardinalityEstimate(10,800,0.9);
        }
    }
}
