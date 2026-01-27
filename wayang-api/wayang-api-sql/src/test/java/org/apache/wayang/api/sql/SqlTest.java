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
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.spatial.data.WGeometry;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.spatial.operators.SpatialFilterOperator;
import org.apache.wayang.spatial.operators.SpatialJoinOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.spatial.function.SpatialPredicate;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


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
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        return new WayangContext(configuration);
    }

//    @Test
//    void testGeoJson() {
//        GeoJsonReader reader = new GeoJsonReader();
//        // Read from file
//        try {
//            File file = new File("/Users/maximilianspeer/wayang/incubator-wayang/wayang-platforms/wayang-java/src/test/resources/geojson-sample.json");
//            FileReader fileReader = new FileReader(file);
//            char[] chars = new char[(int) file.length()];
//            fileReader.read(chars);
//            String geoJson = new String(chars);
//            org.locationtech.jts.geom.Geometry geometry = reader.read(geoJson);
//            System.out.println(geometry);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

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
                new PostgresTableSource("spider_boxes", "id", "geom");

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

        TableSource table1 = new PostgresTableSource("spider_boxes", "id", "x_min", "y_min", "x_max", "y_max", "geom");
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
                (record -> (WGeometry.fromStringInput(record.getString(4)))),
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

        assertEquals(30, collector.size());
    }

    // TODO: Fix and Test
    @Test
    void testSpatialJoinDbSources() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        // Two logical sources over the same table.
        TableSource table1 = new PostgresTableSource("spider_boxes",  "id", "x_min", "y_min", "x_max", "y_max", "geom");
        TableSource table2 = new PostgresTableSource("spider_boxes",  "id", "x_min", "y_min", "x_max", "y_max", "geom");

        // Spatial join on INTERSECTS; both sides use the geom column (index 5).
        SpatialJoinOperator<Record, Record> spatialJoinOperator =
                new SpatialJoinOperator<>(
                        record -> WGeometry.fromStringInput(record.getString(5)),
                        record -> WGeometry.fromStringInput(record.getString(5)),
                        Record.class,
                        Record.class,
                        SpatialPredicate.INTERSECTS
                );

        // Register SQL implementations for both inputs
        spatialJoinOperator.getKeyDescriptor0()
                .withSqlImplementation("spiderdb", "geom");
        spatialJoinOperator.getKeyDescriptor1()
                .withSqlImplementation("spiderdb", "geom");

        spatialJoinOperator.addTargetPlatform(Postgres.platform());

        // Wire up both DB sources as inputs to the spatial join.
        table1.connectTo(0, spatialJoinOperator, 0);
        table2.connectTo(0, spatialJoinOperator, 1);

        // Collect results.
        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink =
                LocalCallbackSink.createCollectingSink(
                        collector,
                        DataSetType.createDefaultUnchecked(Tuple2.class)
                );
        spatialJoinOperator.connectTo(0, sink, 0);

        // Execute the plan.
        wayangContext.execute("PostgreSql spatial join DB-DB", new WayangPlan(sink));

        // Basic sanity check: we should get at least self-intersections.
        assertFalse(collector.isEmpty(), "Spatial join result should not be empty.");

        // Semantic check: every returned pair must actually intersect according to JTS.
        for (Tuple2<Record, Record> pair : collector) {
            Geometry g1 = WGeometry.fromStringInput(pair.field0.getString(1)).getGeometry();
            Geometry g2 = WGeometry.fromStringInput(pair.field1.getString(1)).getGeometry();
            assertTrue(
                    g1.intersects(g2),
                    "Found non-intersecting pair in spatial join result."
            );
        }
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
//        // Basic sanity check: we should get at least self-intersections.
//        assertFalse(collector.isEmpty(), "Spatial join result should not be empty.");
//
//        // Semantic check: every returned pair must actually intersect according to JTS.
//        for (Tuple2<Record, Record> pair : collector) {
//            Geometry g1 = WGeometry.fromStringInput(pair.field0.getString(1)).getGeometry();
//            Geometry g2 = WGeometry.fromStringInput(pair.field1.getString(1)).getGeometry();
//            assertTrue(
//                    g1.intersects(g2),
//                    "Found non-intersecting pair in spatial join result."
//            );
//        }
//    }


    }



    // TODO: Fix and Test
    @Test
    void testJoinDbSources() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        // Two logical sources over the same table.poster_link
        TableSource table1 = new PostgresTableSource("imdb_top_1000",  "series_title", "released_year", "certificate", "runtime", "genre", "imdb_rating", "overview", "meta_score", "director", "star1", "star2", "star3", "star4", "no_of_votes", "gross");
        TableSource table2 = new PostgresTableSource("imdb_top_1000",  "series_title", "released_year", "certificate", "runtime", "genre", "imdb_rating", "overview", "meta_score", "director", "star1", "star2", "star3", "star4", "no_of_votes", "gross");

        // Join on series_title
        JoinOperator<Record, Record, String> joinOperator =
                new JoinOperator<>(
                        record -> record.getString(0),
                        record -> record.getString(0),
                        Record.class,
                        Record.class,
                        String.class
                );

        joinOperator.getKeyDescriptor0()
                .withSqlImplementation("imdb_top_1000", "series_title");
        joinOperator.getKeyDescriptor1()
                .withSqlImplementation("imdb_top_1000", "series_title");

        joinOperator.addTargetPlatform(Postgres.platform());

        // Wire up both DB sources as inputs to the join.
        table1.connectTo(0, joinOperator, 0);
        table2.connectTo(0, joinOperator, 1);

        // Collect results.
        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink =
                LocalCallbackSink.createCollectingSink(
                        collector,
                        DataSetType.createDefaultUnchecked(Tuple2.class)
                );
        joinOperator.connectTo(0, sink, 0);
        // Execute the plan.
        wayangContext.execute("PostgreSql join DB-DB", new WayangPlan(sink));

        // Basic sanity check: we should get at least self-joins.
        assertFalse(collector.isEmpty(), "Join result should not be empty.");
    }
}
