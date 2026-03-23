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
 * Pushdown overhead scaling benchmark: measures Wayang PG pushdown overhead
 * for filter+COUNT and join+COUNT queries at 10k, 100k, and 1M scales.
 * All operators are pushed down to PostgreSQL (same SQL as native PG).
 */
public class PushdownOverheadBenchmark {

    private static final int NUM_RUNS = 5;
    private static final String CSV_FILE = "benchmark_overhead_scaling.csv";

    private static final String FILTER_WKT = "POLYGON((0.30 0.30,0.70 0.30,0.70 0.70,0.30 0.70,0.30 0.30))";
    private static final String[] TABLE_COLUMNS = {"id", "x_min", "y_min", "x_max", "y_max", "geom"};

    // Scale configurations: {table1, table2 (for join)}
    private static final String TABLE_10K = "spider_boxes_10k";
    private static final String TABLE_10K_2 = "spider_boxes_10k_2";
    private static final String TABLE_100K = "spider_boxes";
    private static final String TABLE_100K_2 = "spider_boxes_2";
    private static final String TABLE_1M = "spider_boxes_1m_uniform";
    private static final String TABLE_1M_2 = "spider_boxes_1m_gaussian";

    public static void main(String[] args) throws IOException {
        PrintWriter csv = new PrintWriter(new FileWriter(CSV_FILE, false));
        csv.println("query_name,engine,run,runtime_ms");

        // Filter + COUNT at each scale
        runQuery(csv, "filter_count_10k", () -> buildFilterCountPlan(TABLE_10K));
        runQuery(csv, "filter_count_100k", () -> buildFilterCountPlan(TABLE_100K));
        runQuery(csv, "filter_count_1m", () -> buildFilterCountPlan(TABLE_1M));

        // Join + COUNT at each scale
        runQuery(csv, "join_count_10k", () -> buildJoinCountPlan(TABLE_10K, TABLE_10K_2));
        runQuery(csv, "join_count_100k", () -> buildJoinCountPlan(TABLE_100K, TABLE_100K_2));
        runQuery(csv, "join_count_1m", () -> buildJoinCountPlan(TABLE_1M, TABLE_1M_2));

        csv.close();
        System.out.println("Pushdown overhead benchmark complete. Results written to " + CSV_FILE);
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

    // ========== Filter + COUNT (pushdown to PG) ==========

    private static void buildFilterCountPlan(String tableName) {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource source = new PostgresTableSource(tableName, TABLE_COLUMNS);

        SpatialFilterOperator<Record> spatialFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput(FILTER_WKT)
        );
        spatialFilter.getKeyDescriptor().withSqlImplementation(tableName, "geom");
        spatialFilter.addTargetPlatform(Postgres.platform());
        source.connectTo(0, spatialFilter, 0);

        CountOperator<Record> countOp = new CountOperator<>(DataSetType.createDefaultUnchecked(Record.class));
        countOp.addTargetPlatform(Postgres.platform());
        spatialFilter.connectTo(0, countOp, 0);

        Collection<Long> collector = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector, DataSetType.createDefault(Long.class));
        countOp.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark filter_count " + tableName, new WayangPlan(sink));
        System.out.printf("[wayang] filter_count %s count: %d%n", tableName, collector.iterator().next());
    }

    // ========== Join + COUNT (pushdown to PG) ==========

    private static void buildJoinCountPlan(String table1Name, String table2Name) {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource(table1Name, TABLE_COLUMNS);
        TableSource table2 = new PostgresTableSource(table2Name, TABLE_COLUMNS);

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                record -> WayangGeometry.fromStringInput(record.getString(5)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );
        spatialJoin.getKeyDescriptor0().withSqlImplementation(table1Name, "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation(table2Name, "geom");
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

        wayangContext.execute("Benchmark join_count " + table1Name, new WayangPlan(sink));
        System.out.printf("[wayang] join_count %s x %s count: %d%n", table1Name, table2Name, collector.iterator().next());
    }
}
