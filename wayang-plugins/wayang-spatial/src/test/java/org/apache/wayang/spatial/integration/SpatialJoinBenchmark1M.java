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

/**
 * 1M-row spatial join benchmark: uniform boxes vs Gaussian-centered boxes.
 * All experiments use COUNT(*) as sink to avoid OOM on large result sets.
 */
public class SpatialJoinBenchmark1M {

    private static final int NUM_RUNS = 5;
    private static final String CSV_FILE = "benchmark_results_1m.csv";

    private static final String TABLE_UNIFORM = "spider_boxes_1m_uniform";
    private static final String TABLE_GAUSSIAN = "spider_boxes_1m_gaussian";
    private static final String[] TABLE_COLUMNS = {"id", "x_min", "y_min", "x_max", "y_max", "geom"};

    private static final String FILE_UNIFORM = "boxes_1M_0.001_1.wkt";
    private static final String FILE_GAUSSIAN = "boxes_1M_gaussian_1.wkt";

    public static void main(String[] args) throws IOException {
        PrintWriter csv = new PrintWriter(new FileWriter(CSV_FILE, true));

        // Add header only if file is new
        File f = new File(CSV_FILE);
        if (f.length() == 0 || !f.exists()) {
            csv.println("query_name,engine,run,runtime_ms");
        }

        // Experiment 1: Full SQL pushdown via Wayang
        runQuery(csv, "pg_join_pg_exec", SpatialJoinBenchmark1M::buildPgJoinPgExec);

        // Experiment 2: Data from PG, join in Java
        runQuery(csv, "pg_join_java_exec", SpatialJoinBenchmark1M::buildPgJoinJavaExec);

        // Experiment 3: Both from WKT files, join in Java
        runQuery(csv, "file_join_java", SpatialJoinBenchmark1M::buildFileJoinJava);

        // Experiment 4: Cross-platform PG + file, join in Java
        runQuery(csv, "cross_pg_file_java", SpatialJoinBenchmark1M::buildCrossPgFileJava);

        // Experiment 5: Cross-platform file + PG, join in Java
        runQuery(csv, "cross_file_pg_java", SpatialJoinBenchmark1M::buildCrossFilePgJava);

        // Experiment 6: Cross-platform PG-gaussian (left) + file-uniform (right)
        // Swapped distribution ordering to isolate STRtree index vs probe cost
        runQuery(csv, "cross_pg_gauss_file_uniform", SpatialJoinBenchmark1M::buildCrossPgGaussFileUniform);

        // Experiment 7: Cross-platform file-gaussian (left) + PG-uniform (right)
        runQuery(csv, "cross_file_gauss_pg_uniform", SpatialJoinBenchmark1M::buildCrossFileGaussPgUniform);

        csv.close();
        System.out.println("1M benchmark complete. Results written to " + CSV_FILE);
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

    // ========== Experiment 1: PG join with PG execution (full pushdown) ==========

    private static void buildPgJoinPgExec() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource(TABLE_UNIFORM, TABLE_COLUMNS);
        TableSource table2 = new PostgresTableSource(TABLE_GAUSSIAN, TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation(TABLE_UNIFORM, "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation(TABLE_GAUSSIAN, "geom");
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

        wayangContext.execute("Benchmark pg_join_pg_exec", new WayangPlan(sink));
        System.out.printf("[wayang] pg_join_pg_exec count: %d%n", collector.iterator().next());
    }

    // ========== Experiment 2: PG sources, Java execution ==========

    private static void buildPgJoinJavaExec() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource(TABLE_UNIFORM, TABLE_COLUMNS);
        TableSource table2 = new PostgresTableSource(TABLE_GAUSSIAN, TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        // No SQL implementation, no target platform → runs in Java

        table1.connectTo(0, spatialJoin, 0);
        table2.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        // No target platform → count in Java
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark pg_join_java_exec", new WayangPlan(sink));
        System.out.printf("[wayang] pg_join_java_exec count: %d%n", collector.iterator().next());
    }

    // ========== Experiment 3: File sources, Java execution ==========

    private static void buildFileJoinJava() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TextFileSource fileSource1 = new TextFileSource(wktFileUri(FILE_UNIFORM));
        MapOperator<String, Record> map1 = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource1.connectTo(0, map1, 0);

        TextFileSource fileSource2 = new TextFileSource(wktFileUri(FILE_GAUSSIAN));
        MapOperator<String, Record> map2 = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource2.connectTo(0, map2, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        map1.connectTo(0, spatialJoin, 0);
        map2.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark file_join_java", new WayangPlan(sink));
        System.out.printf("[wayang] file_join_java count: %d%n", collector.iterator().next());
    }

    // ========== Experiment 4: Cross-platform PG (uniform) + file (gaussian) ==========

    private static void buildCrossPgFileJava() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: PG table
        TableSource pgSource = new PostgresTableSource(TABLE_UNIFORM, TABLE_COLUMNS);

        // Right: WKT file
        TextFileSource fileSource = new TextFileSource(wktFileUri(FILE_GAUSSIAN));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        pgSource.connectTo(0, spatialJoin, 0);
        mapToRecord.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_pg_file_java", new WayangPlan(sink));
        System.out.printf("[wayang] cross_pg_file_java count: %d%n", collector.iterator().next());
    }

    // ========== Experiment 5: Cross-platform file (uniform) + PG (gaussian) ==========

    private static void buildCrossFilePgJava() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: WKT file
        TextFileSource fileSource = new TextFileSource(wktFileUri(FILE_UNIFORM));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        // Right: PG table
        TableSource pgSource = new PostgresTableSource(TABLE_GAUSSIAN, TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        mapToRecord.connectTo(0, spatialJoin, 0);
        pgSource.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_file_pg_java", new WayangPlan(sink));
        System.out.printf("[wayang] cross_file_pg_java count: %d%n", collector.iterator().next());
    }

    // ========== Experiment 6: Cross-platform PG-gaussian (left) + file-uniform (right) ==========
    // Swapped vs experiment 4: Gaussian is now probing, uniform is indexed in STRtree

    private static void buildCrossPgGaussFileUniform() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: PG Gaussian table
        TableSource pgSource = new PostgresTableSource(TABLE_GAUSSIAN, TABLE_COLUMNS);

        // Right: WKT uniform file (indexed in STRtree)
        TextFileSource fileSource = new TextFileSource(wktFileUri(FILE_UNIFORM));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        pgSource.connectTo(0, spatialJoin, 0);
        mapToRecord.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_pg_gauss_file_uniform", new WayangPlan(sink));
        System.out.printf("[wayang] cross_pg_gauss_file_uniform count: %d%n", collector.iterator().next());
    }

    // ========== Experiment 7: Cross-platform file-gaussian (left) + PG-uniform (right) ==========
    // Swapped vs experiment 5: Gaussian is now probing, uniform is indexed in STRtree

    private static void buildCrossFileGaussPgUniform() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Left: WKT Gaussian file
        TextFileSource fileSource = new TextFileSource(wktFileUri(FILE_GAUSSIAN));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        // Right: PG uniform table (indexed in STRtree)
        TableSource pgSource = new PostgresTableSource(TABLE_UNIFORM, TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        mapToRecord.connectTo(0, spatialJoin, 0);
        pgSource.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark cross_file_gauss_pg_uniform", new WayangPlan(sink));
        System.out.printf("[wayang] cross_file_gauss_pg_uniform count: %d%n", collector.iterator().next());
    }
}
