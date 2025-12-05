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
        final List<Record> result = outputs[0].<Record>provideStream().toList();
        assertEquals(3, result.size(), "Expected 3 geometries in the sample GeoJSON file.");
    }
}