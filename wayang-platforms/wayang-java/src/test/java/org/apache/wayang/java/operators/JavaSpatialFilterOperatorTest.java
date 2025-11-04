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
import org.apache.wayang.basic.data.SpatialRecord;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.channels.JavaChannelInstance;
//import org.apache.wayang.postgres.Postgres;
//import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaSpatialFilterOperator}.
 */
class JavaSpatialFilterOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    void testExecution() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.2, 0.00, 0.20);
        Geometry queryGeometry = geometryFactory.toGeometry(envelope);

        SpatialRecord hits = createSpatialRecord(1, geometryFactory.createPoint(new Coordinate(0.1, 0.1)));
        SpatialRecord misses = createSpatialRecord(2, geometryFactory.createPoint(new Coordinate(1.0, 1.0)));
        SpatialRecord boundary = createSpatialRecord(3, geometryFactory.createPoint(new Coordinate(0.15, 0.05)));

        Stream<SpatialRecord> inputStream = Stream.of(hits, misses, boundary);

        JavaSpatialFilterOperator<SpatialRecord> filterOperator =
                new JavaSpatialFilterOperator<>(
                        "INTERSECTS",
                        1,
                        queryGeometry
                );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(filterOperator, inputs, outputs);

        final List<SpatialRecord> result = outputs[0].<SpatialRecord>provideStream().collect(Collectors.toList());
        assertEquals(2, result.size());
        assertEquals(asList(hits, boundary), result);
    }

//    @Test
//    void testConnect() {

//
//    }

    private static SpatialRecord createSpatialRecord(Object id, Geometry geometry) {
        SpatialRecord record = new SpatialRecord();
        record.addField(id);
        record.addField(geometry);
        return record;
    }
}
