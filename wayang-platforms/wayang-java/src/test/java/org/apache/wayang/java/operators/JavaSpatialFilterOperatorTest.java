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
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.*;

import java.net.URL;
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
    void testExecutionIntersects() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.2, 0.00, 0.20);
        Geometry queryGeometry = geometryFactory.toGeometry(envelope);

        Record hits = createRecord(1, geometryFactory.createPoint(new Coordinate(0.1, 0.1)));
        Record misses = createRecord(2, geometryFactory.createPoint(new Coordinate(1.0, 1.0)));
        Record boundary = createRecord(3, geometryFactory.createPoint(new Coordinate(0.15, 0.05)));

        Stream<Record> inputStream = Stream.of(hits, misses, boundary);

        JavaSpatialFilterOperator<Record> filterOperator = createSpatialFilterOperator(
                SpatialPredicate.INTERSECTS,
                WGeometry.fromGeometry(queryGeometry)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(filterOperator, inputs, outputs);

        final List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(2, result.size());
        assertEquals(asList(hits, boundary), result);
    }

    @Test
    void testContains() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: small square [0,1] x [0,1]
        Geometry reference = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));

        // Candidate 1 strictly contains reference
        Geometry contains = gf.toGeometry(new Envelope(-1.0, 2.0, -1.0, 2.0));
        // Candidate 2 is inside reference (does not contain it)
        Geometry inside = gf.toGeometry(new Envelope(0.25, 0.75, 0.25, 0.75));
        // Candidate 3 is disjoint
        Geometry disjoint = gf.toGeometry(new Envelope(2.0, 3.0, 2.0, 3.0));

        Record rContains = createRecord(1, contains);
        Record rInside = createRecord(2, inside);
        Record rDisjoint = createRecord(3, disjoint);

        Stream<Record> input = Stream.of(rContains, rInside, rDisjoint);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.CONTAINS,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        // Only the geometry that strictly contains the reference should remain.
        assertEquals(asList(rContains), result);
    }

    @Test
    void testWithin() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: big square [-1,2] x [-1,2]
        Geometry reference = gf.toGeometry(new Envelope(-1.0, 2.0, -1.0, 2.0));

        // Candidate 1: inside reference
        Geometry inside = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));
        // Candidate 2: outside reference
        Geometry outside = gf.toGeometry(new Envelope(3.0, 4.0, 3.0, 4.0));

        Record rInside = createRecordWithWGeometry(1, inside);  // store as WGeometry to exercise that path
        Record rOutside = createRecordWithWGeometry(2, outside);

        Stream<Record> input = Stream.of(rInside, rOutside);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.WITHIN,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(asList(rInside), result);
    }

    @Test
    void testTouches() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: square [0,1] x [0,1]
        Geometry reference = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));

        // Candidate 1: adjacent square [1,2] x [0,1], sharing the vertical edge x=1
        Geometry touching = gf.toGeometry(new Envelope(1.0, 2.0, 0.0, 1.0));
        // Candidate 2: separate square [2,3] x [0,1]
        Geometry nonTouching = gf.toGeometry(new Envelope(2.0, 3.0, 0.0, 1.0));
        // Candidate 3: overlapping/intersecting square [-0.5,0.5] x [0,1] (touches+overlaps, but intersects interior)
        Geometry overlapping = gf.toGeometry(new Envelope(-0.5, 0.5, 0.0, 1.0));

        Record rTouching = createRecord(1, touching);
        Record rNonTouching = createRecord(2, nonTouching);
        Record rOverlapping = createRecord(3, overlapping);

        Stream<Record> input = Stream.of(rTouching, rNonTouching, rOverlapping);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.TOUCHES,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        // Only the purely touching geometry should remain.
        assertEquals(asList(rTouching), result);
    }

    @Test
    void testOverlaps() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: rectangle [0,2] x [0,2]
        Geometry reference = gf.toGeometry(new Envelope(0.0, 2.0, 0.0, 2.0));

        // Candidate 1: overlapping rectangle [1,3] x [0,2]
        Geometry overlapping = gf.toGeometry(new Envelope(1.0, 3.0, 0.0, 2.0));
        // Candidate 2: containing rectangle [-1,3] x [-1,3] (contains, not overlaps)
        Geometry containing = gf.toGeometry(new Envelope(-1.0, 3.0, -1.0, 3.0));
        // Candidate 3: disjoint rectangle [3,4] x [0,1]
        Geometry disjoint = gf.toGeometry(new Envelope(3.0, 4.0, 0.0, 1.0));

        Record rOverlapping = createRecord(1, overlapping);
        Record rContaining = createRecord(2, containing);
        Record rDisjoint = createRecord(3, disjoint);

        Stream<Record> input = Stream.of(rOverlapping, rContaining, rDisjoint);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.OVERLAPS,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(asList(rOverlapping), result);
    }

    @Test
    void testCrosses() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: square [0,1] x [0,1]
        Geometry reference = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));

        // Candidate 1: horizontal line crossing the square at y=0.5
        Coordinate[] coordsCrossing = new Coordinate[]{
                new Coordinate(-1.0, 0.5),
                new Coordinate(2.0, 0.5)
        };
        LineString crossingLine = gf.createLineString(coordsCrossing);

        // Candidate 2: horizontal line above the square
        Coordinate[] coordsNonCrossing = new Coordinate[]{
                new Coordinate(-1.0, 2.0),
                new Coordinate(2.0, 2.0)
        };
        LineString nonCrossingLine = gf.createLineString(coordsNonCrossing);

        Record rCrossing = createRecord(1, crossingLine);
        Record rNonCrossing = createRecord(2, nonCrossingLine);

        Stream<Record> input = Stream.of(rCrossing, rNonCrossing);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.CROSSES,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(asList(rCrossing), result);
    }

    @Test
    void testDisjoint() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: square [0,1] x [0,1]
        Geometry reference = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));

        // Candidate 1: disjoint square [2,3] x [2,3]
        Geometry disjoint = gf.toGeometry(new Envelope(2.0, 3.0, 2.0, 3.0));
        // Candidate 2: intersecting square [0.5,1.5] x [0.5,1.5]
        Geometry intersecting = gf.toGeometry(new Envelope(0.5, 1.5, 0.5, 1.5));

        Record rDisjoint = createRecord(1, disjoint);
        Record rIntersecting = createRecord(2, intersecting);

        Stream<Record> input = Stream.of(rDisjoint, rIntersecting);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.DISJOINT,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(asList(rDisjoint), result);
    }

    @Test
    void testEqualsTopo() {
        GeometryFactory gf = new GeometryFactory();

        // Reference: square [0,1] x [0,1]
        Geometry reference = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));

        // Candidate 1: same square (topologically equal)
        Geometry same = gf.toGeometry(new Envelope(0.0, 1.0, 0.0, 1.0));
        // Candidate 2: different square
        Geometry different = gf.toGeometry(new Envelope(0.0, 2.0, 0.0, 1.0));

        Record rSame = createRecord(1, same);
        Record rDifferent = createRecord(2, different);

        Stream<Record> input = Stream.of(rSame, rDifferent);

        JavaSpatialFilterOperator<Record> op = createSpatialFilterOperator(
                SpatialPredicate.EQUALS,
                WGeometry.fromGeometry(reference)
        );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(input)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(op, inputs, outputs);

        List<Record> result = outputs[0].<Record>provideStream().collect(Collectors.toList());
        assertEquals(asList(rSame), result);
    }

    @Test
    void testWithinGeoJson() {
        final String testFileName = "/geojson-sample.json";

        // Prepare the source.
        final URL inputUrl = this.getClass().getResource(testFileName);
        final JavaGeoJsonFileSource source = new JavaGeoJsonFileSource(
                inputUrl.toString());

        // Execute.
        final JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
        final JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        evaluate(source, inputs, outputs);

        // Feed to spatial filter
        JavaSpatialFilterOperator<Record> filterOperator =
                createSpatialFilterOperator(
                        SpatialPredicate.WITHIN,
                        WGeometry.fromGeometry(new GeometryFactory().toGeometry(new Envelope(-180, 180, -90, 90))),
                        0
                );
        JavaChannelInstance[] filterInputs = new JavaChannelInstance[]{outputs[0]};
        JavaChannelInstance[] filterOutputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(filterOperator, filterInputs, filterOutputs);

        final List<Record> result = filterOutputs[0].<Record>provideStream().collect(Collectors.toList());
        // All 3 geometries in the sample GeoJSON file should be within the global envelope
        assertEquals(3, result.size());
    }

    @Test
    void testReadLocalCsv() {
        final String testFileName = "/uniform.csv";

        // Prepare the source.
        final URL inputUrl = this.getClass().getResource(testFileName);
        final JavaTextFileSource source = new JavaTextFileSource(
                inputUrl.toString());

        // Execute.
        final JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
        final JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        evaluate(source, inputs, outputs);

        // Feed to spatial filter
        Stream<Record> records = outputs[0].<String>provideStream()
                .map(line -> {
                    String[] tokens = line.split(",");
                    double x = Double.parseDouble(tokens[0]);
                    double y = Double.parseDouble(tokens[1]);
                    GeometryFactory gf = new GeometryFactory();
                    Point point = gf.createPoint(new Coordinate(x, y));
                    return createRecord(line, point);
                });
        JavaSpatialFilterOperator<Record> filterOperator =
                createSpatialFilterOperator(
                        SpatialPredicate.INTERSECTS,
                        WGeometry.fromGeometry(new GeometryFactory().toGeometry(new Envelope(0.4, 0.6, 0.4, 0.6)))
                );
        JavaChannelInstance[] filterInputs = new JavaChannelInstance[]{createStreamChannelInstance(records)};
        JavaChannelInstance[] filterOutputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(filterOperator, filterInputs, filterOutputs);

        final List<Record> result = filterOutputs[0].<Record>provideStream().collect(Collectors.toList());
        // There should be 2 points within the envelope
        assertEquals(2, result.size());
    }

    private static JavaSpatialFilterOperator<Record> createSpatialFilterOperator(SpatialPredicate relation,
                                                                                 WGeometry referenceGeometry) {
        return new JavaSpatialFilterOperator<>(
                relation,
                JavaSpatialFilterOperatorTest::extractWGeometry,
                DataSetType.createDefaultUnchecked(Record.class),
                referenceGeometry
        );
    }

    private static JavaSpatialFilterOperator<Record> createSpatialFilterOperator(SpatialPredicate relation,
                                                                                 WGeometry referenceGeometry,
                                                                                 int index) {
        return new JavaSpatialFilterOperator<>(
                relation,
                record -> extractWGeometry(record, index),
                DataSetType.createDefaultUnchecked(Record.class),
                referenceGeometry
        );
    }

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
