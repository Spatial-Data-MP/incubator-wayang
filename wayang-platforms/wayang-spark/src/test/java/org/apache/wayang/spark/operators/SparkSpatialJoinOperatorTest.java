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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.channels.RddChannel;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkSpatialJoinOperatorTest extends SparkOperatorTestBase {

    @Test
    void testExecutionJoinIntersects() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.2, 0.00, 0.20);
        Geometry queryGeometry = geometryFactory.toGeometry(envelope);

        final List<WGeometry> inputValues1 = Arrays.asList(
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.0 0.10,0.10 0.10,0.10 0.10,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.20 0.20,0.20 0.30,0.30 0.30,0.30 0.20,0.20 0.20))"),
                WGeometry.fromStringInput("POLYGON((0.40 0.00,0.40 0.50,0.50 0.50,0.50 0.40,0.40 0.00))")
                //WGeometry.fromStringInput("POLYGON((0.00 0.00,0.10 0.00,0.10 0.10,0.00 0.10,0.00 0.00))")
        );


        final List<WGeometry> inputValues2 = Arrays.asList(
                WGeometry.fromStringInput("POLYGON((0.90 0.90,0.90 1.00,1.00 1.00,1.00 0.90,0.90 0.90))"),
                WGeometry.fromStringInput("POLYGON((0.20 0.20,0.20 0.30,0.30 0.30,0.30 0.20,0.20 0.20))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.80,0.00 0.90,0.10 0.90,0.10 0.80,0.00 0.80))")
                //WGeometry.fromStringInput("POLYGON((0.00 0.00,0.10 0.00,0.10 0.10,0.00 0.10,0.00 0.00))")
        );

        RddChannel.Instance input1 = this.createRddChannelInstance(inputValues1);
        RddChannel.Instance input2 = this.createRddChannelInstance(inputValues2);
        RddChannel.Instance output = this.createRddChannelInstance();


        SparkSpatialJoinOperator<WGeometry, WGeometry> spatialJoinOperator = new SparkSpatialJoinOperator<WGeometry, WGeometry>(
                geom1 -> geom1,
                geom2 -> geom2,
                WGeometry.class,
                WGeometry.class,
                SpatialPredicate.INTERSECTS
        );

        ChannelInstance[] inputs = new ChannelInstance[]{input1, input2};
        ChannelInstance[] outputs = new ChannelInstance[]{output};
        this.evaluate(spatialJoinOperator, inputs, outputs);

        final List<Record> result = output.<Record>provideRdd().collect();
        assertEquals(1, result.size());
//        assertEquals(asList(hits, boundary), result);
    }
}
