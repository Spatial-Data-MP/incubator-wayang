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
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.geojson.GeoJsonReader;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/spatialdb"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

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

        SpatialFilterOperator<Record> spatialFilterOperator = new SpatialFilterOperator<Record>(
                SpatialPredicate.INTERSECTS,
                (record -> (WGeometry.fromStringInput(record.getString(1)))),
                DataSetType.createDefaultUnchecked(Record.class),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.4 0.00,0.4 0.4,0.00 0.4,0.00 0.00))"));

        spatialFilterOperator.getKeyDescriptor().withSqlImplementation("spatialdb", "geom");
        spatialFilterOperator.addTargetPlatform(Spark.platform());
        spider.connectTo(0,spatialFilterOperator,0);

        Collection<Tuple2<Integer, WGeometry>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Integer, WGeometry>> sink
                = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefaultUnchecked(Record.class));
        spatialFilterOperator.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

        assertEquals(19, collector.size());
    }

    @Test
    void testSpatialFilterWithTuple() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        ///  Scalar Geometry
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.4, 0.00, 0.40);
        Geometry geom2 = geometryFactory.toGeometry(envelope);

        TableSource spider =
                new PostgresTableSource("spider", "id", "geom");

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

        SpatialFilterOperator<Tuple2<Integer, WGeometry>> spatialFilterOperator = new SpatialFilterOperator<Tuple2<Integer, WGeometry>>(
                SpatialPredicate.INTERSECTS,
                Tuple2::getField1,
                DataSetType.createDefaultUnchecked(Tuple2.class),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.4 0.00,0.4 0.4,0.00 0.4,0.00 0.00))"));

        spatialFilterOperator.addTargetPlatform(Java.platform());
        spider.connectTo(0,mapToTuple,0);
        mapToTuple.connectTo(0,spatialFilterOperator,0);

        Collection<Tuple2<Integer, WGeometry>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Integer, WGeometry>> sink
                = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialFilterOperator.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);
        assertEquals(19, collector.size());
    }

    @Test
    void testSpatialJoin() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        TableSource table1 = new PostgresTableSource("spider", "id", "geom");
//        TableSource table2 = new PostgresTableSource("spider", "id", "geom");

        // Input polygons: nested axis-aligned squares.
        final List<WGeometry> inputValues = Arrays.asList(
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.30 0.00,0.30 0.30,0.00 0.30,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.20 0.00,0.20 0.20,0.00 0.20,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.10 0.00,0.10 0.10,0.00 0.10,0.00 0.00))")
        );
        CollectionSource<WGeometry> inputCollection = new CollectionSource<>(inputValues, WGeometry.class);


        SpatialJoinOperator<Record, WGeometry> spatialJoinOperator = new SpatialJoinOperator<Record, WGeometry>(
                (record -> (WGeometry.fromStringInput(record.getString(1)))),
                (wgeometry -> wgeometry),
                Record.class,
                WGeometry.class,
                SpatialPredicate.INTERSECTS
                );
        table1.connectTo(0, spatialJoinOperator, 0);
        inputCollection.connectTo(0, spatialJoinOperator, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink
                = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoinOperator.connectTo(0, sink, 0);
        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

        assertEquals(31, collector.size());
    }


    // Alt
    @Test
    void testSpatialFilter() {
                WayangPlan wayangPlan;
        //// Db Connection, local db credentials!
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/spatialdb"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");



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
