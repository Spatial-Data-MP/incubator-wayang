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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.SpatialRelation;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link SpatialFilterOperator}.
 */
public class JavaSpatialFilterOperator<Type>
        extends SpatialFilterOperator<Type>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param relation the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public JavaSpatialFilterOperator(SpatialRelation relation,
                                     FunctionDescriptor.SerializableFunction<Type, WGeometry> keyExtractor,
                                     Class<Type> inputClass,
                                     WGeometry geometry,
                                     String geometryColumnSqlName) {
        super(relation, keyExtractor, inputClass, geometry, geometryColumnSqlName);
        /*if (this.geometryColumnIndex < 0) {
            throw new IllegalArgumentException("Column index must be >= 0.");
        }*/
    }

    public JavaSpatialFilterOperator(SpatialFilterOperator<Type> that) {
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




        final Predicate<Type> filterPredicate = this.buildSpatialPredicate(javaExecutor);
        ((StreamChannel.Instance) outputs[0]).accept(
                ((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(filterPredicate)
        );

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Predicate<Type> buildSpatialPredicate(JavaExecutor javaExecutor) {
        final Geometry reference = this.referenceGeometry.getGeometry();
        final Function<Type, WGeometry> keyExtractor = javaExecutor.getCompiler().compile(this.keyDescriptor);

        return input -> {
//            Geometry candidate = this.extractGeometry(record);
            return this.relation.test(keyExtractor.apply(input).getGeometry(), reference);
//            this.keyDescriptor.getJavaImplementation();
//
//            Geometry candidate = this.keyDescriptor.getJavaImplementation();
//            if (candidate == null) {
//                return false;
//            }
//            return this.relation.test(candidate, reference);
        };
    }

//    private Geometry extractGeometry(org.apache.wayang.basic.data.Record record) {
//        final Object field = record.getField(this.geometryColumnIndex);
//
//        // Code to convert
//        if (field instanceof WGeometry) {
//            return ((WGeometry) field).getGeometry();
//        }
//        else
//        {
//            return WGeometry.fromStringInput((String) (field.toString())).getGeometry();
//        }
//
//        if (field instanceof Geometry) {
//            // Already a Geometry object
//            return (Geometry) field;
//        } else if (field instanceof PGobject) {
//            // Handle PostGIS geometry stored as a PGobject
//            final PGobject pgObj = (PGobject) field;
//            final String value = pgObj.getValue();
//            if (value == null) {
//                return null;
//            }
//            // Convert hex string to binary and parse as WKB
//            try {
//                return (new WKBReader()).read(DatatypeConverter.parseHexBinary(value));
//            } catch (ParseException e) {
//                throw new RuntimeException(e);
//            }
//        } else if (field instanceof String) {
//            // Handle raw hex string geometry
//            try {
//                return (new WKBReader()).read(DatatypeConverter.parseHexBinary((String) field));
//            } catch (ParseException e) {
//                throw new RuntimeException(e);
//            }
//        } else {
//            throw new ClassCastException("Field at index " + this.geometryColumnIndex + " is not a Geometry or PGobject: " + field);
//        }

//        return ((WGeometry) record.getField(1)).getGeometry();
//        try {
//            return record.getGeometry(this.geometryColumnIndex, new WKBReader());
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
//    }

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
