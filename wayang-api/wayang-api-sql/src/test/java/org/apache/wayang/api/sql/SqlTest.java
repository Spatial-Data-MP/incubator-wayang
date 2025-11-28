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
//import org.apache.wayang.basic.data.SpatialRecord;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.SpatialRelation;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.postgresql.core.v3.QueryExecutorImpl;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    void testGeoJson() {
        GeoJsonReader reader = new GeoJsonReader();
        // Read from file
        try {
            File file = new File("/Users/maximilianspeer/wayang/incubator-wayang/wayang-platforms/wayang-java/src/test/resources/geojson-sample.json");
            FileReader fileReader = new FileReader(file);
            char[] chars = new char[(int) file.length()];
            fileReader.read(chars);
            String geoJson = new String(chars);
            org.locationtech.jts.geom.Geometry geometry = reader.read(geoJson);
            System.out.println(geometry);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testSpatialFilterOperator() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        //// Debugging might be useful, set level to "FINEST" to see actual db query strings
//        Logger logger = Logger.getLogger(QueryExecutorImpl.class.getName());
//        ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(Level.FINEST);
//        // handler.setFilter(record -> record.getMessage() != null && record.getMessage().contains("query="));
//        logger.addHandler(handler);
//        logger.setLevel(Level.FINEST);


        ///  Scalar Geometry
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.4, 0.00, 0.40);
        Geometry geom2 = geometryFactory.toGeometry(envelope);

        TableSource spider =
                new PostgresTableSource("spider", "id", "geom");

//        GeoJsonFileSource spiderFileSource =
//                new GeoJsonFileSource("data/spider_points.geojson",
//                        Record.class,
//                        "id", "geom");
//        MapOperator<WGeometry, Record> mapToRecord = new MapOperator<WGeometry, Record>(
//                (wGeometry -> {
//                    Object[] values = new Object[2];
//                    values[0] = wGeometry.getAttribute();
//                    values[1] = wGeometry;
//                    return new Record(values);
//                }),
//                WGeometry.class,
//                Record.class
//        );
//        spiderFileSource.connectTo(0, mapToRecord, 0);


//        MapOperator<Record, SpatialRecord> mapToSpatial = new MapOperator<Record,SpatialRecord>(
//                (record -> new SpatialRecord(record.getValues())), Record.class, SpatialRecord.class
//        );
//        spider.connectTo(0, mapToSpatial, 0);

//        MapOperator<Record, Record> mapToWGeometry = new MapOperator<Record, Record>(
//                (record -> {
//                    Object[] values = Arrays.copyOf(record.getValues(), record.getValues().length);
//                    String wkb = values[1].toString();
//                    values[1] = new org.apache.wayang.basic.data.WGeometry("POLYGON((0.19793055784917613 0.1257896454307232,0.20481163436045868 0.1257896454307232,0.20481163436045868 0.12801131389541112,0.19793055784917613 0.12801131389541112,0.19793055784917613 0.1257896454307232))\n");
//                    values[1] = WGeometry.fromStringInput(wkb);
//                    return new Record(values);
//                }),
//                Record.class,
//                Record.class
//        );

        MapOperator<Record, Tuple2<Integer, WGeometry>> mapToTuple = new MapOperator<Record, Tuple2<Integer, WGeometry>>(
                record -> {
                    Tuple2<Integer, WGeometry> tuple = new Tuple2<>();
                    tuple.field0 = record.getInt(0);
                    tuple.field1 = WGeometry.fromStringInput(record.getField(1).toString());
                    return tuple;
                },
                Record.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        SpatialFilterOperator<Tuple2> spatialFilterOperator = new SpatialFilterOperator<Tuple2>(
                SpatialRelation.INTERSECTS,
                Tuple2::getField0,
                Tuple2.class,
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.4 0.00,0.4 0.4,0.00 0.4,0.00 0.00))"),
                "geom");

        spatialFilterOperator.addTargetPlatform(Spark.platform());
//        spider.connectTo(0,mapToWGeometry,0);
        spider.connectTo(0, spatialFilterOperator, 0);
//        mapToWGeometry.connectTo(0,spatialFilterOperator,0);

        Collection<Record> collector = new ArrayList<>();
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
        spatialFilterOperator.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

        assertEquals(19, collector.size());
    }


    // Alt
    @Test
    void testSpatialFilter() {
                WayangPlan wayangPlan;
        //// Db Connection, local db credentials!
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5433/postgres"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "1234");



        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin());
//        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
//                .withJobName("Filter Test")
//                .withUdfJarOf(TestSpatialOperators.class);


        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.2, 0.00, 0.20);
        Geometry geom2 = geometryFactory.toGeometry(envelope);

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


//        MapOperator<Record, SpatialRecord> mapToSpatial = new MapOperator<Record,SpatialRecord>(
//                (record -> new SpatialRecord(record.getValues())), Record.class, SpatialRecord.class
//        );
//        mapToSpatial.addTargetPlatform(Postgres.platform());

//        customer.connectTo(0, mapToSpatial, 0);

//        SpatialFilterOperator spatialFilterOperator = new SpatialFilterOperator(
//                "INTERSECTS",
//                1,
//                geom2,
////                , DataSetType.createDefault(Record.class)
//                "");

//        FilterOperator<Record> simpleFilter = new FilterOperator<Record>(
//                (record -> (record.getInt(0)) > 20), Record.class
//        );
//        simpleFilter.getPredicateDescriptor().withSqlImplementation("id > 30");
//
//        simpleFilter.addTargetPlatform(Postgres.platform());

//        spatialFilterOperator.addTargetPlatform(Postgres.platform());
//        customer.connectTo(0,spatialFilterOperator,0);
//        mapToSpatial.connectTo(0,spatialFilterOperator,0);
//        spatialFilterOperator.connectTo(0, sink, 0);

//        customer.connectTo(0, simpleFilter, 0);
//        simpleFilter.connectTo(0, sink, 0);



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

        assertEquals(2, collector.size());

//        List<RelDataType> columnTypes = Arrays.asList(null, null);
//        JavaCSVTableSource<Record> textFileSource = new JavaCSVTableSource<>(
//                "/data/spider_points.csv",
//                DataSetType.createDefault(Record.class),
//                columnTypes
//        );


    }
}
