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

package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.SpatialRecord;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.locationtech.jts.geom.Geometry;
import org.apache.wayang.basic.data.Record;

import java.util.Locale;
import java.util.Objects;


/**
 * This operator returns a new dataset after filtering by applying predicateDescriptor.
 */
public class SpatialFilterOperator extends UnaryToUnaryOperator<SpatialRecord, SpatialRecord> {


    protected final String filterType;
    protected final int geometryColumnIndex;
    protected final Geometry referenceGeometry;

    /**
     * Creates a new instance.
     */
    public SpatialFilterOperator(String filterType, Integer columnIndex, Geometry geometry) {
        super(DataSetType.createDefault(SpatialRecord.class), DataSetType.createDefault(SpatialRecord.class), true);
        this.filterType = (filterType == null ? "INTERSECTS" : filterType).toUpperCase(Locale.ROOT);
        this.geometryColumnIndex = columnIndex == null ? 0 : columnIndex;
//        this.referenceGeometry = Objects.requireNonNull(geometry, "Reference geometry must not be null.");
        this.referenceGeometry = geometry;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SpatialFilterOperator(SpatialFilterOperator that) {
        super(that);
        this.filterType = that.filterType;
        this.geometryColumnIndex = that.geometryColumnIndex;
        this.referenceGeometry = that.referenceGeometry;
    }


//    public DataSetType<Type> getType() {
//        return this.getInputType();
//    }

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
