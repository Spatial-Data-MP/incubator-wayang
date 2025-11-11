/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.SpatialRecord;
import org.apache.wayang.basic.data.geometry.BoundingBoxGeometry;
import org.apache.wayang.basic.data.geometry.Geometry;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class SqlTest {


    public static void main(String[] args) {
        WayangPlan wayangPlan;
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/imdb");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        Collection<Record> collector = new ArrayList<>();

        TableSource customer = new PostgresTableSource("person");
        MapOperator<Record, Record> projection = MapOperator.createProjection(
                Record.class,
                Record.class,
                "name");

        /*int[] fields = new int[]{1};
        MapOperator<Record, Record> projection = new MapOperator(
                new WayangProjectVisitor.MapFunctionImpl(fields),
                Record.class,
                Record.class);*/

        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
        customer.connectTo(0,projection,0);
        projection.connectTo(0,sink,0);


        wayangPlan = new WayangPlan(sink);

        wayangContext.execute("PostgreSql test", wayangPlan);


        int count = 10;
        for(Record r : collector) {
            System.out.println(r.getField(0).toString());
            if(--count == 0 ) {
                break;
            }
        }
        System.out.println("Done");








    }

    WayangContext getTestWayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5433/postgres"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "1234");

        return new WayangContext(configuration);
    }

    @Test
    void testSpatialFilterOperator() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin());

        ///  Scalar Geometry
        Geometry geom2 = BoundingBoxGeometry.fromExtents(0.00, 0.2, 0.00, 0.20);

        TableSource spider = new PostgresTableSource("spider", "id", "geom");

        MapOperator<Record, SpatialRecord> mapToSpatial = new MapOperator<Record,SpatialRecord>(
                (record -> new SpatialRecord(record.getValues())), Record.class, SpatialRecord.class
        );
        spider.connectTo(0, mapToSpatial, 0);

        SpatialFilterOperator spatialFilterOperator = new SpatialFilterOperator(
                "INTERSECTS",
                1,
                geom2
        );
        mapToSpatial.connectTo(0,spatialFilterOperator,0);

        Collection<Record> collector = new ArrayList<>();
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
        spatialFilterOperator.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

        assertEquals(2, collector.size());
    }


    // Alt, doppelter Code
    @Test
    void connectToTest() {


        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin());
//        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
//                .withJobName("Filter Test")
//                .withUdfJarOf(TestSpatialOperators.class);


        Geometry geom2 = BoundingBoxGeometry.fromExtents(0.00, 0.2, 0.00, 0.20);
        Collection<org.apache.wayang.basic.data.Record> collector = new ArrayList<>();

        TableSource customer = new PostgresTableSource("spider", "id", "geom");
//        MapOperator<org.apache.wayang.basic.data.Record, org.apache.wayang.basic.data.Record> projection = MapOperator.createProjection(
//                org.apache.wayang.basic.data.Record.class,
//                org.apache.wayang.basic.data.Record.class,
//                "name");

        /*int[] fields = new int[]{1};
        MapOperator<Record, Record> projection = new MapOperator(
                new WayangProjectVisitor.MapFunctionImpl(fields),
                Record.class,
                Record.class);*/

        LocalCallbackSink<org.apache.wayang.basic.data.Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
//        customer.connectTo(0,projection,0);


        MapOperator<Record, SpatialRecord> mapToSpatial = new MapOperator<Record,SpatialRecord>(
                (record -> new SpatialRecord(record.getValues())), Record.class, SpatialRecord.class
        );
//        mapToSpatial.addTargetPlatform(Postgres.platform());

        customer.connectTo(0, mapToSpatial, 0);

        SpatialFilterOperator spatialFilterOperator = new SpatialFilterOperator(
                "INTERSECTS",
                1,
                geom2
//                , DataSetType.createDefault(Record.class)
        );

//        FilterOperator<Record> simpleFilter = new FilterOperator<Record>(
//                (record -> (record.getInt(0)) > 20), Record.class
//        );
//        simpleFilter.getPredicateDescriptor().withSqlImplementation("id > 30");
//
//        simpleFilter.addTargetPlatform(Postgres.platform());

//        spatialFilterOperator.addTargetPlatform(Postgres.platform());
//        customer.connectTo(0,spatialFilterOperator,0);
        mapToSpatial.connectTo(0,spatialFilterOperator,0);
        spatialFilterOperator.connectTo(0, sink, 0);

//        customer.connectTo(0, simpleFilter, 0);
//        simpleFilter.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

//        int count = 10;
//        for(Record r : collector) {
//            System.out.println(r.getField(0).toString());
//            if(--count == 0 ) {
//                break;
//            }
//        }
        System.out.println("Done");

        assertEquals(2, collector.size());


    }


}
