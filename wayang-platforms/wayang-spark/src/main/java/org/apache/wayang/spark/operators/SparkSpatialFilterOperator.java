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

package org.apache.wayang.spark.operators;

import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Spark implementation of the {@link SpatialFilterOperator}.
 */
public class SparkSpatialFilterOperator<Type>
        extends SpatialFilterOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param relation the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     *
     */
    public SparkSpatialFilterOperator(SpatialPredicate relation,
                                      FunctionDescriptor.SerializableFunction<Type, WGeometry> keyExtractor,
                                      DataSetType<Type> inputClassDatasetType,
                                      WGeometry geometry) {
        super(relation, keyExtractor, inputClassDatasetType, geometry);
//        if (this.geometryColumnIndex < 0) {
//            throw new IllegalArgumentException("Column index must be >= 0.");
//        }
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkSpatialFilterOperator(SpatialFilterOperator that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Geometry reference = this.referenceGeometry == null ? null : this.referenceGeometry.getGeometry();
        if (reference == null) {
            throw new IllegalStateException("Reference geometry must not be null for spatial filtering.");
        }

        final JavaRDD<Record> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();
        final JavaRDD<Geometry> geometryRdd = inputRdd
//                .map(this::attachGeometryUserData)
//                .map((input -> (WGeometry.fromStringInput("POLYGON((0.00 0.00,0.4 0.00,0.4 0.4,0.00 0.4,0.00 0.00))")).getGeometry()))
//                .map((record -> ((WGeometry.fromStringInput(record.getString(1))).getGeometry())))
                .map(SparkSpatialFilterOperator::attachGeometryUserData)
//                .map((input -> reference))
                .filter(Objects::nonNull)
                ;

        final SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
        spatialRDD.setRawSpatialRDD(geometryRdd);
        spatialRDD.analyze();

        final JavaRDD<Record> outputRdd = this.applySedonaSpatialFilter(spatialRDD, reference);
        this.name(outputRdd);
        ((RddChannel.Instance) outputs[0]).accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private JavaRDD<Record> applySedonaSpatialFilter(SpatialRDD<Geometry> spatialRDD, Geometry reference) {
        final org.apache.sedona.core.spatialOperator.SpatialPredicate predicate = this.toSedonaPredicate(this.relation);
        if (predicate == null) {
            // Fallback to JTS if we cannot express the relation via Sedona.
            return spatialRDD.getRawSpatialRDD()
                    .filter(geom -> geom != null && this.relation.test(geom, reference))
                    .map(geom -> (Record) geom.getUserData());
        }

        try {
            final JavaRDD<Geometry> matched = RangeQuery.SpatialRangeQuery(spatialRDD, reference, predicate, false);
            if (this.relation == SpatialPredicate.DISJOINT) {
                // Sedona does not expose DISJOINT directly; invert INTERSECTS results.
                final JavaRDD<Record> intersecting = matched.map(geom -> (Record) geom.getUserData());
                final JavaRDD<Record> all = spatialRDD.getRawSpatialRDD().map(geom -> (Record) geom.getUserData());
                return all.subtract(intersecting);
            }
            return matched.map(geom -> (Record) geom.getUserData());
        } catch (Exception e) {
            throw new RuntimeException("Sedona range query failed for spatial filter.", e);
        }
    }

    public static Geometry attachGeometryUserData(Record record) {
        return (WGeometry.fromStringInput(record.getString(1))).getGeometry();
        // TODO: attach user data
//        final Geometry geometry = this.extractGeometry(record);
//        if (geometry != null) {
//            geometry.setUserData(record);
//        }
//        return geometry;
    }

//    private Geometry extractGeometry(Record record) {
//        final Object field = record.getField(1);
//        if (field == null) {
//            return null;
//        }
//        if (field instanceof Geometry) {
//            return (Geometry) field;
//        }
//        if (field instanceof WGeometry) {
//            return ((WGeometry) field).getGeometry();
//        }
//        return WGeometry.fromStringInput(field.toString()).getGeometry();
//    }

    private org.apache.sedona.core.spatialOperator.SpatialPredicate toSedonaPredicate(SpatialPredicate relation) {
        switch (relation) {
            case INTERSECTS:
            case DISJOINT:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.INTERSECTS;
            case CONTAINS:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.CONTAINS;
            case WITHIN:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.WITHIN;
            case TOUCHES:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.TOUCHES;
            case OVERLAPS:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.OVERLAPS;
            case CROSSES:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.CROSSES;
            case EQUALS:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.EQUALS;
            default:
                return null;
        }
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.spatialfilter.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
