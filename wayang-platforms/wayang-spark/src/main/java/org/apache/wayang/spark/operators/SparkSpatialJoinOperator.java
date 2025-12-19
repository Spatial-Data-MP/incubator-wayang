package org.apache.wayang.spark.operators;

import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.spark.channels.RddChannel;
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

    public SparkSpatialJoinOperator(SparkSpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(){
        // This is a placeholder for the actual implementation.
        return null;
    }


    private org.apache.sedona.core.spatialOperator.SpatialPredicate toSedonaPredicate(SpatialPredicate predicate) {
        switch (predicate) {
            case INTERSECTS:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.INTERSECTS;
            case CONTAINS:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.CONTAINS;
            case WITHIN:
                return org.apache.sedona.core.spatialOperator.SpatialPredicate.WITHIN;
            default:
                throw new IllegalArgumentException("Unsupported spatial predicate: " + predicate);
        }
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
