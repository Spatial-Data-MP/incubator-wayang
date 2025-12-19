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
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.*;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaSpatialFilterOperator}.
 */
class JavaSpatialJoinOperatorTest extends JavaExecutionOperatorTestBase {


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

        JavaSpatialJoinOperator<WGeometry, WGeometry> spatialJoinOperator = new JavaSpatialJoinOperator<WGeometry, WGeometry>(
                geom1 -> geom1,
                geom2 -> geom2,
                WGeometry.class,
                WGeometry.class,
                SpatialPredicate.INTERSECTS
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(inputValues1.stream()),
                createStreamChannelInstance(inputValues2.stream())
                };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(spatialJoinOperator, inputs, outputs);


        final List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(1, result.size());

//        assertEquals(asList(hits, boundary), result);

//        final List<Record> result = outputs.<Record>provideRdd().collect();
//        assertEquals(1, result.size());
//        assertEquals(asList(hits, boundary), result);
    }

//    @Test
//    void testIntersectsJoin() {
//        GeometryFactory geometryFactory = new GeometryFactory();
//        Envelope envelope = new Envelope(0.00, 0.2, 0.00, 0.20);
//        Geometry queryGeometry = geometryFactory.toGeometry(envelope);
//
//        Record hits = createRecord(1, geometryFactory.createPoint(new Coordinate(0.1, 0.1)));
//        Record misses = createRecord(2, geometryFactory.createPoint(new Coordinate(1.0, 1.0)));
//        Record boundary = createRecord(3, geometryFactory.createPoint(new Coordinate(0.15, 0.05)));
//
//        Stream<Record> inputStream = Stream.of(hits, misses, boundary);
//
//        JavaSpatialJoinOperator<Record> filterOperator = new JavaSpatialJoinOperator(
//
//        );
//
//        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
//        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
//        evaluate(filterOperator, inputs, outputs);
//
//        final List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
//        assertEquals(2, result.size());
//        assertEquals(asList(hits, boundary), result);
//    }

//    private static JavaSpatialFilterOperator<Record> createSpatialFilterOperator(SpatialPredicate relation,
//                                                                                 WGeometry referenceGeometry,
//                                                                                 int index) {
//        return new JavaSpatialFilterOperator<>(
//                relation,
//                record -> extractWGeometry(record, index),
//                DataSetType.createDefaultUnchecked(Record.class),
//                referenceGeometry
//        );
//    }

    private static WGeometry extractWGeometry(Record record) {
        Object field = record.getField(1);
        if (field instanceof WGeometry) {
            return (WGeometry) field;
        }
        return WGeometry.fromGeometry((Geometry) field);
    }

    private static WGeometry extractWGeometry(Record record, int index) {
        Object field = record.getField(index);
        if (field instanceof WGeometry) {
            return (WGeometry) field;
        }
        return WGeometry.fromGeometry((Geometry) field);
    }

    private static Record createRecord(Object id, Geometry geometry) {
        Record record = new Record();
        record.addField(id);
        record.addField(geometry);
        return record;
    }

    private static Record createRecordWithWGeometry(Object id, Geometry geometry) {
        Record record = new Record();
        record.addField(id);
        record.addField(WGeometry.fromGeometry(geometry));
        return record;
    }
}
