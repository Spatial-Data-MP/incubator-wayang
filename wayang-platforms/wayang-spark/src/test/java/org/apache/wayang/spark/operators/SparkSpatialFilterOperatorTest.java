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

class SparkSpatialFilterOperatorTest extends SparkOperatorTestBase {

    @Test
    void testExecutionIntersects() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.2, 0.00, 0.20);
        Geometry queryGeometry = geometryFactory.toGeometry(envelope);

        Record hits = createRecord(1, geometryFactory.createPoint(new Coordinate(0.1, 0.1)));
        Record misses = createRecord(2, geometryFactory.createPoint(new Coordinate(1.0, 1.0)));
        Record boundary = createRecord(3, geometryFactory.createPoint(new Coordinate(0.15, 0.05)));

        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(hits, misses, boundary));
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkSpatialFilterOperator<Record> filterOperator = createSpatialFilterOperator(
                SpatialPredicate.INTERSECTS,
                WGeometry.fromGeometry(queryGeometry)
        );

        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};
        this.evaluate(filterOperator, inputs, outputs);

        final List<Record> result = output.<Record>provideRdd().collect();
        assertEquals(2, result.size());
        assertEquals(asList(hits, boundary), result);
    }

    private static SparkSpatialFilterOperator<Record> createSpatialFilterOperator(SpatialPredicate relation,
                                                                                  WGeometry referenceGeometry) {
        return new SparkSpatialFilterOperator<>(
                relation,
                SparkSpatialFilterOperatorTest::extractWGeometry,
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

    private static Record createRecord(Object id, Geometry geometry) {
        Record record = new Record();
        record.addField(id);
        record.addField(geometry);
        geometry.setUserData(record);
        return record;
    }
}
