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

package org.apache.wayang.spatial.integration;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spatial.data.WayangGeometry;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

public class SpatialBenchmark {

    private static final int NUM_RUNS = 10;
    private static final String CSV_FILE = "benchmark_results.csv";

    private static final String FILTER1_WKT = "POLYGON((0.30 0.30,0.70 0.30,0.70 0.70,0.30 0.70,0.30 0.30))";
    private static final String FILTER2_WKT = "POLYGON((0.40 0.40,0.60 0.40,0.60 0.60,0.40 0.60,0.40 0.40))";

    private static final String[] TABLE_COLUMNS = {"id", "x_min", "y_min", "x_max", "y_max", "geom"};

    public static void main(String[] args) throws IOException {
        PrintWriter csv = new PrintWriter(new FileWriter(CSV_FILE, false));
        csv.println("query_name,engine,run,runtime_ms");

        // Existing COUNT(*) benchmarks
        runQuery(csv, "spatial_filter", SpatialBenchmark::buildSpatialFilterPlan);
        runQuery(csv, "spatial_join", SpatialBenchmark::buildSpatialJoinPlan);
        runQuery(csv, "filtered_join", SpatialBenchmark::buildFilteredJoinPlan);

        // Benchmark 1: Cross-platform join (Postgres + WKT file)
        runQuery(csv, "cross_platform_filtered_join", SpatialBenchmark::buildCrossPlatformFilteredJoinPlan);
        runQuery(csv, "cross_platform_join", SpatialBenchmark::buildCrossPlatformJoinPlan);

        // Benchmark 2: Java vs Postgres execution (collect results)
        runQuery(csv, "postgres_filtered_join", SpatialBenchmark::buildPostgresFilteredJoinCollectPlan);
        runQuery(csv, "java_filtered_join", SpatialBenchmark::buildJavaFilteredJoinCollectPlan);
        runQuery(csv, "postgres_join_10k", SpatialBenchmark::buildPostgresJoin10kCollectPlan);
        runQuery(csv, "java_join_10k", SpatialBenchmark::buildJavaJoin10kCollectPlan);
        runQuery(csv, "cross_platform_filtered_join_10k", SpatialBenchmark::buildCrossPlatformFilteredJoin10kPlan);
        runQuery(csv, "postgres_filtered_join_10k", SpatialBenchmark::buildPostgresFilteredJoin10kCollectPlan);
        runQuery(csv, "java_filtered_join_10k", SpatialBenchmark::buildJavaFilteredJoin10kCollectPlan);

        csv.close();
        System.out.println("Wayang benchmark complete. Results written to " + CSV_FILE);
    }

    private static void runQuery(PrintWriter csv, String queryName, Runnable planRunner) {
        double total = 0;
        for (int i = 1; i <= NUM_RUNS; i++) {
            long start = System.nanoTime();
            planRunner.run();
            long elapsed = System.nanoTime() - start;
            double ms = elapsed / 1_000_000.0;
            total += ms;
            String line = String.format("%s,wayang,%d,%.2f", queryName, i, ms);
            csv.println(line);
            csv.flush();
            System.out.printf("[wayang] %s run %d: %.2f ms%n", queryName, i, ms);
        }
        double avg = total / NUM_RUNS;
        csv.println(String.format("%s,wayang,avg,%.2f", queryName, avg));
        csv.flush();
        System.out.printf("[wayang] %s avg: %.2f ms%n", queryName, avg);
    }

    private static WayangContext createContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5433/spiderdb");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");
        return new WayangContext(configuration);
    }

    private static String wktFileUri(String filename) {
        return new File("wayang-plugins/wayang-spatial/" + filename).toURI().toString();
    }

    // ========== Existing COUNT(*) benchmarks ==========

    private static void buildSpatialFilterPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource spider = new PostgresTableSource("spider_boxes", TABLE_COLUMNS);

        SpatialFilterOperator<Record> spatialFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        spatialFilter.getKeyDescriptor().withSqlImplementation("spider_boxes", "geom");
        spatialFilter.addTargetPlatform(Postgres.platform());
        spider.connectTo(0, spatialFilter, 0);

        CountOperator<Record> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Record.class));
        countOp.addTargetPlatform(Postgres.platform());
        spatialFilter.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark spatial_filter", new WayangPlan(sink));
        System.out.printf("[wayang] spatial_filter count: %d%n", collector.iterator().next());
    }

    private static void buildSpatialJoinPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes", TABLE_COLUMNS);
        TableSource table2 = new PostgresTableSource("spider_boxes_2", TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation("spider_boxes", "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation("spider_boxes_2", "geom");
        spatialJoin.addTargetPlatform(Postgres.platform());

        table1.connectTo(0, spatialJoin, 0);
        table2.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        countOp.addTargetPlatform(Postgres.platform());
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark spatial_join", new WayangPlan(sink));
        System.out.printf("[wayang] spatial_join count: %d%n", collector.iterator().next());
    }

    private static void buildFilteredJoinPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Source 1: spider_boxes filtered
        TableSource table1 = new PostgresTableSource("spider_boxes", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter1 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        filter1.getKeyDescriptor().withSqlImplementation("spider_boxes", "geom");
        table1.connectTo(0, filter1, 0);

        // Source 2: spider_boxes_2 filtered
        TableSource table2 = new PostgresTableSource("spider_boxes_2", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter2 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        filter2.getKeyDescriptor().withSqlImplementation("spider_boxes_2", "geom");
        table2.connectTo(0, filter2, 0);

        // Spatial join on filtered results
        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation("spider_boxes", "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation("spider_boxes_2", "geom");
        spatialJoin.addTargetPlatform(Postgres.platform());

        filter1.connectTo(0, spatialJoin, 0);
        filter2.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        countOp.addTargetPlatform(Postgres.platform());
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark filtered_join", new WayangPlan(sink));
        System.out.printf("[wayang] filtered_join count: %d%n", collector.iterator().next());
    }

    // ========== Benchmark 1: Cross-Platform Join (Postgres + WKT File) ==========

    private static void buildCrossPlatformFilteredJoinPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: Postgres table with spatial filter (pushdown)
        TableSource pgSource = new PostgresTableSource("spider_boxes", TABLE_COLUMNS);
        SpatialFilterOperator<Record> pgFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        pgFilter.getKeyDescriptor().withSqlImplementation("spider_boxes", "geom");
        pgFilter.addTargetPlatform(Postgres.platform());
        pgSource.connectTo(0, pgFilter, 0);

        // Right: WKT file → MapOperator(line → Record) → spatial filter (Java)
        TextFileSource fileSource = new TextFileSource(wktFileUri("boxes_100k_2.wkt"));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        SpatialFilterOperator<Record> fileFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        mapToRecord.connectTo(0, fileFilter, 0);

        // Spatial join in Java
        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        pgFilter.connectTo(0, spatialJoin, 0);
        fileFilter.connectTo(0, spatialJoin, 1);

        // Collect results
        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_platform_filtered_join", new WayangPlan(sink));
        System.out.printf("[wayang] cross_platform_filtered_join results: %d%n", collector.size());
    }

    private static void buildCrossPlatformJoinPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: Postgres table (no filter)
        TableSource pgSource = new PostgresTableSource("spider_boxes_10k", TABLE_COLUMNS);

        // Right: WKT file → MapOperator(line → Record), no filter
        TextFileSource fileSource = new TextFileSource(wktFileUri("boxes_10k_2.wkt"));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        // Spatial join in Java
        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        pgSource.connectTo(0, spatialJoin, 0);
        mapToRecord.connectTo(0, spatialJoin, 1);

        // Collect results
        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_platform_join", new WayangPlan(sink));
        System.out.printf("[wayang] cross_platform_join results: %d%n", collector.size());
    }

    // ========== Benchmark 2: Java vs Postgres Execution ==========

    // --- Path A: Join in Postgres, collect in Java ---

    private static void buildPostgresFilteredJoinCollectPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter1 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        filter1.getKeyDescriptor().withSqlImplementation("spider_boxes", "geom");
        filter1.addTargetPlatform(Postgres.platform());
        table1.connectTo(0, filter1, 0);

        TableSource table2 = new PostgresTableSource("spider_boxes_2", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter2 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        filter2.getKeyDescriptor().withSqlImplementation("spider_boxes_2", "geom");
        filter2.addTargetPlatform(Postgres.platform());
        table2.connectTo(0, filter2, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation("spider_boxes", "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation("spider_boxes_2", "geom");
        spatialJoin.addTargetPlatform(Postgres.platform());

        filter1.connectTo(0, spatialJoin, 0);
        filter2.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark postgres_filtered_join", new WayangPlan(sink));
        System.out.printf("[wayang] postgres_filtered_join results: %d%n", collector.size());
    }

    // --- Path B: Convert to Java first, join in Java ---

    private static void buildJavaFilteredJoinCollectPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter1 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        // No SQL implementation, no target platform → runs in Java
        table1.connectTo(0, filter1, 0);

        TableSource table2 = new PostgresTableSource("spider_boxes_2", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter2 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        // No SQL implementation, no target platform → runs in Java
        table2.connectTo(0, filter2, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        // No SQL implementation, no target platform → runs in Java

        filter1.connectTo(0, spatialJoin, 0);
        filter2.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark java_filtered_join", new WayangPlan(sink));
        System.out.printf("[wayang] java_filtered_join results: %d%n", collector.size());
    }

    // --- 10k unfiltered variants ---

    private static void buildPostgresJoin10kCollectPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes_10k", TABLE_COLUMNS);
        TableSource table2 = new PostgresTableSource("spider_boxes_10k_2", TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation("spider_boxes_10k", "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation("spider_boxes_10k_2", "geom");
        spatialJoin.addTargetPlatform(Postgres.platform());

        table1.connectTo(0, spatialJoin, 0);
        table2.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark postgres_join_10k", new WayangPlan(sink));
        System.out.printf("[wayang] postgres_join_10k results: %d%n", collector.size());
    }

    private static void buildJavaJoin10kCollectPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes_10k", TABLE_COLUMNS);
        TableSource table2 = new PostgresTableSource("spider_boxes_10k_2", TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        // No SQL implementation, no target platform → runs in Java

        table1.connectTo(0, spatialJoin, 0);
        table2.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark java_join_10k", new WayangPlan(sink));
        System.out.printf("[wayang] java_join_10k results: %d%n", collector.size());
    }

    // --- 10k filtered variants ---

    private static void buildCrossPlatformFilteredJoin10kPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: Postgres 10k table with spatial filter (pushdown)
        TableSource pgSource = new PostgresTableSource("spider_boxes_10k", TABLE_COLUMNS);
        SpatialFilterOperator<Record> pgFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        pgFilter.getKeyDescriptor().withSqlImplementation("spider_boxes_10k", "geom");
        pgFilter.addTargetPlatform(Postgres.platform());
        pgSource.connectTo(0, pgFilter, 0);

        // Right: WKT file → MapOperator(line → Record) → spatial filter (Java)
        TextFileSource fileSource = new TextFileSource(wktFileUri("boxes_10k_2.wkt"));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        SpatialFilterOperator<Record> fileFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        mapToRecord.connectTo(0, fileFilter, 0);

        // Spatial join in Java
        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        pgFilter.connectTo(0, spatialJoin, 0);
        fileFilter.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_platform_filtered_join_10k", new WayangPlan(sink));
        System.out.printf("[wayang] cross_platform_filtered_join_10k results: %d%n", collector.size());
    }

    private static void buildPostgresFilteredJoin10kCollectPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes_10k", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter1 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        filter1.getKeyDescriptor().withSqlImplementation("spider_boxes_10k", "geom");
        filter1.addTargetPlatform(Postgres.platform());
        table1.connectTo(0, filter1, 0);

        TableSource table2 = new PostgresTableSource("spider_boxes_10k_2", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter2 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        filter2.getKeyDescriptor().withSqlImplementation("spider_boxes_10k_2", "geom");
        filter2.addTargetPlatform(Postgres.platform());
        table2.connectTo(0, filter2, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation("spider_boxes_10k", "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation("spider_boxes_10k_2", "geom");
        spatialJoin.addTargetPlatform(Postgres.platform());

        filter1.connectTo(0, spatialJoin, 0);
        filter2.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark postgres_filtered_join_10k", new WayangPlan(sink));
        System.out.printf("[wayang] postgres_filtered_join_10k results: %d%n", collector.size());
    }

    private static void buildJavaFilteredJoin10kCollectPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes_10k", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter1 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER1_WKT)
        );
        table1.connectTo(0, filter1, 0);

        TableSource table2 = new PostgresTableSource("spider_boxes_10k_2", TABLE_COLUMNS);
        SpatialFilterOperator<Record> filter2 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER2_WKT)
        );
        table2.connectTo(0, filter2, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        filter1.connectTo(0, spatialJoin, 0);
        filter2.connectTo(0, spatialJoin, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark java_filtered_join_10k", new WayangPlan(sink));
        System.out.printf("[wayang] java_filtered_join_10k results: %d%n", collector.size());
    }
}
