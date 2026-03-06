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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

public class SpatialBenchmark {

    private static final int NUM_RUNS = 5;
    private static final String CSV_FILE = "benchmark_results.csv";

    public static void main(String[] args) throws IOException {
        PrintWriter csv = new PrintWriter(new FileWriter(CSV_FILE, false));
        csv.println("query_name,engine,run,runtime_ms");

        runQuery(csv, "spatial_filter", SpatialBenchmark::buildSpatialFilterPlan);
        runQuery(csv, "spatial_join", SpatialBenchmark::buildSpatialJoinPlan);
        runQuery(csv, "filtered_join", SpatialBenchmark::buildFilteredJoinPlan);

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

    private static void buildSpatialFilterPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource spider = new PostgresTableSource("spider_boxes", "id", "x_min", "y_min", "x_max", "y_max", "geom");

        SpatialFilterOperator<Record> spatialFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))")
        );
        spatialFilter.getKeyDescriptor().withSqlImplementation("spider_boxes", "geom");
        spatialFilter.addTargetPlatform(Postgres.platform());
        spider.connectTo(0, spatialFilter, 0);

        Collection<Record> collector = new ArrayList<>();
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Record.class));
        spatialFilter.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark spatial_filter", new WayangPlan(sink));
    }

    private static void buildSpatialJoinPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes", "id", "x_min", "y_min", "x_max", "y_max", "geom");
        TableSource table2 = new PostgresTableSource("spider_boxes_2", "id", "x_min", "y_min", "x_max", "y_max", "geom");

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

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark spatial_join", new WayangPlan(sink));
    }

    private static void buildFilteredJoinPlan() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        // Source 1: spider_boxes filtered
        TableSource table1 = new PostgresTableSource("spider_boxes", "id", "x_min", "y_min", "x_max", "y_max", "geom");
        SpatialFilterOperator<Record> filter1 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))")
        );
        filter1.getKeyDescriptor().withSqlImplementation("spider_boxes", "geom");
        table1.connectTo(0, filter1, 0);

        // Source 2: spider_boxes_2 filtered
        TableSource table2 = new PostgresTableSource("spider_boxes_2", "id", "x_min", "y_min", "x_max", "y_max", "geom");
        SpatialFilterOperator<Record> filter2 = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.20 0.00,0.20 0.20,0.00 0.20,0.00 0.00))")
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

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoin.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark filtered_join", new WayangPlan(sink));
    }
}
