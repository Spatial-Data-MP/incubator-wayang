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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.function.SpatialRelation;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
 import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.postgresql.util.PGobject;

import javax.management.relation.RelationType;
import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link SpatialFilterOperator}.
 */
public class SparkSpatialFilterOperator
        extends SpatialFilterOperator
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param filterType the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public SparkSpatialFilterOperator(SpatialRelation relation, Integer columnIndex, WGeometry geometry) {
        super(relation, columnIndex, geometry, "");
        if (this.geometryColumnIndex < 0) {
            throw new IllegalArgumentException("Column index must be >= 0.");
        }
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

        final Function<Record, Boolean> spatialPredicate = this.createSpatialPredicate();
        final JavaRDD<Record> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();
        final JavaRDD<Record> outputRdd = inputRdd.filter(spatialPredicate);
        this.name(outputRdd);
        ((RddChannel.Instance) outputs[0]).accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Function<Record, Boolean> createSpatialPredicate() {

//        final SpatialRelation relation = this.relation;
//        final int columnIndex = this.geometryColumnIndex;
//        final Geometry reference = this.referenceGeometry.getGeometry();

        return (record -> true);
//        return record -> {
//            if (reference == null) {
//                return false;
//            }
//            final Geometry candidate = extractGeometry(record, columnIndex);
//            if (candidate == null) {
//                return false;
//            }
//            return this.relation.test(candidate, reference);
//        };
    }
//
//    private Geometry extractGeometry(Record record, int columnIndex) {
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
//    }

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
