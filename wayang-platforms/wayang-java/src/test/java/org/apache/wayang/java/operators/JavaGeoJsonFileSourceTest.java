package org.apache.wayang.java.operators;

import com.google.gson.JsonElement;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;

import static org.apache.wayang.java.operators.JavaExecutionOperatorTestBase.createStreamChannelInstance;
import static org.apache.wayang.java.operators.JavaExecutionOperatorTestBase.evaluate;
import static org.junit.jupiter.api.Assertions.*;

class JavaGeoJsonFileSourceTest extends JavaExecutionOperatorTestBase {

    @Test
    void testReadLocalGeoJson() throws Exception {
        final String testFileName = "/geojson-sample.json";

        final URL inputUrl = this.getClass().getResource(testFileName);
        System.out.println("* " + inputUrl + " *");
        final JavaGeoJsonFileSource source = new JavaGeoJsonFileSource(
                inputUrl.toString());

        // Execute
        final JavaChannelInstance[] inputs = new JavaChannelInstance[] {};
        final JavaChannelInstance[] outputs = new JavaChannelInstance[] {
                createStreamChannelInstance()
        };
        evaluate(source, inputs, outputs);

        //verify outcome
        final List<JsonElement> result = outputs[0].<JsonElement>provideStream().toList();
        assertEquals(3, result.size(), "Expected 3 geometries in the sample GeoJSON file.");
    }
}