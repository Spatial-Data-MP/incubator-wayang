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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.data.SpatialRecord;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * Java implementation of the {@link FilterOperator}.
 */
public class JavaSpatialFilterOperator
        extends SpatialFilterOperator
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param filterType the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public JavaSpatialFilterOperator(String filterType, Integer columnIndex, Geometry geometry) {
        super(filterType, columnIndex, geometry /*, DataSetType.createDefault(Record.class)*/);
        if (this.geometryColumnIndex < 0) {
            throw new IllegalArgumentException("Column index must be >= 0.");
        }
    }

    public JavaSpatialFilterOperator(SpatialFilterOperator that) {
        super(that);
    }


//    /**
//     * Copies an instance (exclusive of broadcasts).
//     *
//     * @param that that should be copied
//     */
//    public JavaSpatialFilterOperator(FilterOperator<Type> that) {
//        super(that);
//    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        final Predicate<SpatialRecord> filterPredicate = this.buildSpatialPredicate();
        ((StreamChannel.Instance) outputs[0]).accept(
                ((JavaChannelInstance) inputs[0]).<SpatialRecord>provideStream().filter(filterPredicate)
        );

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Predicate<SpatialRecord> buildSpatialPredicate() {
        return record -> {
//            Geometry candidate = this.extractGeometry((SpatialRecord) record);
            Geometry candidate = this.extractGeometry(record);
            if (candidate == null) {
                return false;
            }
            switch (this.filterType) {
                case "INTERSECTS":
                    return candidate.intersects(this.referenceGeometry);
                case "CONTAINS":
                    return candidate.contains(this.referenceGeometry);
                case "WITHIN":
                    return candidate.within(this.referenceGeometry);
                default:
                    throw new IllegalArgumentException("Unsupported spatial filter type: " + this.filterType);
            }
        };
    }

    private Geometry extractGeometry(SpatialRecord record) {
//        return (Geometry) record.getField(1);
        try {
            return record.getGeometry(this.geometryColumnIndex, new WKBReader());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
//    public String getLoadProfileEstimatorConfigurationKey() {
//        return "wayang.java.filter.load";
//    }
//
//    @Override
//    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
//        final Optional<LoadProfileEstimator> optEstimator =
//                JavaExecutionOperator.super.createLoadProfileEstimator(configuration);
//        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.predicateDescriptor, configuration);
//        return optEstimator;
//    }

//    @Override
//    protected ExecutionOperator createCopy() {
//        return new JavaSpatialFilterOperator<>(this.getInputType(), this.getPredicateDescriptor());
//    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
