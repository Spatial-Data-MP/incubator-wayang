package org.apache.wayang.java.operators;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.wayang.basic.operators.GeoJsonFileSource;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java execution operator that parses a GeoJSON document and emits each feature as a {@link WGeometry}.
 *
 * Each emitted WGeometry is created from the feature JSON text (so the geojson field of WGeometry
 * contains the feature with its geometry, type and properties).
 */
public class JavaGeoJsonFileSource extends GeoJsonFileSource implements JavaExecutionOperator {

    public JavaGeoJsonFileSource(String inputUrl) {
        super(inputUrl);
    }

    public JavaGeoJsonFileSource(GeoJsonFileSource that) {
        super(that);
    }

    public static Stream<JsonElement> readFeatureCollectionFromFile(final String path) {
        try {
            final URI uri = URI.create(path);
            final String content = Files.readString(Path.of(uri), StandardCharsets.UTF_8);
            final JsonElement jsonElement = JsonParser.parseString(content);
            final JsonArray features = jsonElement.getAsJsonObject().getAsJsonArray("features");
            return StreamSupport.stream(features.spliterator(), false);
        } catch (final Exception e) {
            throw new WayangException(e);
        }
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {

        assert outputs.length == this.getNumOutputs();

        final String path = this.getInputUrl();
        final Stream<JsonElement> featureStream = readFeatureCollectionFromFile(path);
        final Stream<WGeometry> wGeometryStream = featureStream.map(WGeometry::fromJsonInput);

        ((StreamChannel.Instance) outputs[0]).accept(wGeometryStream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public JavaGeoJsonFileSource copy() {
        return new JavaGeoJsonFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
