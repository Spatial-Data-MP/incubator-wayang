/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.spatial.operators.spark;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.spatial.data.WGeometry;
import org.apache.wayang.spatial.operators.SpatialJoinOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.spatial.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SparkSpatialJoinOperator<InputType0, InputType1>
        extends SpatialJoinOperator<InputType0, InputType1>
        implements SparkExecutionOperator {

    public SparkSpatialJoinOperator(
            TransformationDescriptor<InputType0, WGeometry> keyDescriptor0,
            TransformationDescriptor<InputType1, WGeometry> keyDescriptor1,
            DataSetType<InputType0> inputType0,
            DataSetType<InputType1> inputType1,
            SpatialPredicate predicate) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1, predicate);
    }

    public SparkSpatialJoinOperator(FunctionDescriptor.SerializableFunction<InputType0, WGeometry> keyExtractor0,
                               FunctionDescriptor.SerializableFunction<InputType1, WGeometry> keyExtractor1,
                               Class<InputType0> input0Class,
                               Class<InputType1> input1Class,
                               SpatialPredicate predicate) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, predicate);
    }

    public SparkSpatialJoinOperator(SparkSpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
    }

    public SparkSpatialJoinOperator(SpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, SparkExecutor sparkExecutor, OptimizationContext.OperatorContext operatorContext) {
        final JavaRDD<InputType0> leftIn = ((RddChannel.Instance) inputs[0]).provideRdd();
        final JavaRDD<InputType1> rightIn = ((RddChannel.Instance) inputs[1]).provideRdd();

        final FunctionDescriptor.SerializableFunction<InputType0, WGeometry> keyExtractor0 =
                (FunctionDescriptor.SerializableFunction<InputType0, WGeometry>) this.keyDescriptor0.getJavaImplementation();
        final FunctionDescriptor.SerializableFunction<InputType1, WGeometry> keyExtractor1 =
                (FunctionDescriptor.SerializableFunction<InputType1, WGeometry>) this.keyDescriptor1.getJavaImplementation();


        final JavaRDD<Geometry> leftInGeometry = leftIn.map((InputType0 in1) -> {
            final WGeometry wGeom = keyExtractor0.apply(in1);
            Geometry geom = wGeom.getGeometry();
            geom.setUserData(in1);
            return geom;
        });

        final JavaRDD<Geometry> rightInGeometry = rightIn.map((InputType1 in2) -> {
            final WGeometry wGeom = keyExtractor1.apply(in2);
            Geometry geom = wGeom.getGeometry();
            geom.setUserData(in2);
            return geom;
        });


        // TODO: consider cardinality and tell sedona which index to use
        final SpatialRDD<Geometry> spatialRDDLeft = new SpatialRDD<>();
        final SpatialRDD<Geometry> spatialRDDRight = new SpatialRDD<>();

        try {
            spatialRDDLeft.setRawSpatialRDD(leftInGeometry);
            spatialRDDRight.setRawSpatialRDD(rightInGeometry);

            spatialRDDLeft.analyze();
            spatialRDDLeft.analyze();

            // TODO: fix num partitions max |spatialRDDLeft|/2
            spatialRDDLeft.spatialPartitioning(GridType.KDBTREE, 1);
            spatialRDDRight.spatialPartitioning(spatialRDDLeft.getPartitioner());

            JavaPairRDD<Geometry, Geometry> sedonaJoin = JoinQuery.spatialJoin(
                    spatialRDDLeft,
                    spatialRDDRight,
                    new JoinQuery.JoinParams(false, toSedonaPredicate(this.predicate))
            );
            final JavaRDD<Tuple2<InputType0, InputType1>> outputRdd =
                    sedonaJoin.map(geoTuple ->
                            new Tuple2<>(
                                    (InputType0) geoTuple._1().getUserData(),
                                    (InputType1) geoTuple._2().getUserData()
                            )
                    );

            ((RddChannel.Instance) outputs[0]).accept(outputRdd, sparkExecutor);
            return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: fix code duplication with filter
    private org.apache.sedona.core.spatialOperator.SpatialPredicate toSedonaPredicate(SpatialPredicate predicate) {
        return switch (predicate) {
            case INTERSECTS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.INTERSECTS;
            case CONTAINS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.CONTAINS;
            case WITHIN -> org.apache.sedona.core.spatialOperator.SpatialPredicate.WITHIN;
            case TOUCHES -> org.apache.sedona.core.spatialOperator.SpatialPredicate.TOUCHES;
            case OVERLAPS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.OVERLAPS;
            case CROSSES -> org.apache.sedona.core.spatialOperator.SpatialPredicate.CROSSES;
            case EQUALS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.EQUALS;
            default -> throw new IllegalStateException("Unsupported spatial filter predicate: " + predicate);
        };
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.spatialjoin.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }
}
