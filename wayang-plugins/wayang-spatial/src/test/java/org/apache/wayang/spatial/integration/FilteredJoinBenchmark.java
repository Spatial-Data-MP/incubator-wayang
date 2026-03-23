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
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spatial.data.WayangGeometry;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Filtered cross-platform spatial join benchmark: applies spatial filters before joining.
 * Left source from Postgres (filter pushed down), right source from WKT file (filter in Java),
 * then spatial join in Java with TextFileSink output.
 */
public class FilteredJoinBenchmark {

    private static final int NUM_RUNS = 5;
    private static final String CSV_FILE = "benchmark_filtered_join.csv";

    private static final String FILTER_WKT = "POLYGON((13.3 52.48, 13.5 52.48, 13.5 52.55, 13.3 52.55, 13.3 52.48))";

    private static final String TABLE_UNIFORM = "osm_berlin_parks";
    private static final String[] TABLE_COLUMNS = {"id", "geom"};

    private static final String FILE_GAUSSIAN = "osm_berlin_sports.wkt";
    private static final String OUTPUT_FILE = "berlin_parks_sports_join.wkt";

    public static void main(String[] args) throws IOException {
        PrintWriter csv = new PrintWriter(new FileWriter(CSV_FILE, false));
        csv.println("query_name,engine,run,runtime_ms");

        runQuery(csv, "filtered_cross_pg_file_java", FilteredJoinBenchmark::buildFilteredCrossPgFileJava);

        csv.close();
        System.out.println("Filtered join benchmark complete. Results written to " + CSV_FILE);
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

    private static void buildFilteredCrossPgFileJava() {
        WayangContext wayangContext = createContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        WayangGeometry filterGeometry = WayangGeometry.fromStringInput(FILTER_WKT);

        // Left: PG table → SpatialFilter (pushdown to PG)
        TableSource pgSource = new PostgresTableSource(TABLE_UNIFORM, TABLE_COLUMNS);

        SpatialFilterOperator<Record> pgFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(1)),
                DataSetType.createDefaultUnchecked(Record.class),
                filterGeometry
        );
        pgFilter.getKeyDescriptor().withSqlImplementation(TABLE_UNIFORM, "geom");
        pgSource.connectTo(0, pgFilter, 0);

        // Right: WKT file → Map → SpatialFilter (Java)
        TextFileSource fileSource = new TextFileSource(wktFileUri(FILE_GAUSSIAN));
        MapOperator<String, Record> mapToRecord = new MapOperator<>(
                line -> new Record(new Object[]{line}),
                String.class, Record.class
        );
        fileSource.connectTo(0, mapToRecord, 0);

        SpatialFilterOperator<Record> fileFilter = new SpatialFilterOperator<>(
                SpatialPredicate.INTERSECTS,
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                DataSetType.createDefaultUnchecked(Record.class),
                filterGeometry
        );
        mapToRecord.connectTo(0, fileFilter, 0);

        // SpatialJoin on filtered streams (Java)
        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(1)),
                record -> WayangGeometry.fromStringInput(record.getString(0)),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        pgFilter.connectTo(0, spatialJoin, 0);
        fileFilter.connectTo(0, spatialJoin, 1);

        // Map join results to WKT: "leftWKT;rightWKT"
        @SuppressWarnings("unchecked")
        MapOperator<Tuple2<Record, Record>, String> toWkt = new MapOperator<>(
                tuple -> {
                    String leftWkt = WayangGeometry.fromStringInput(tuple.field0.getString(1)).toWKT();
                    String rightWkt = WayangGeometry.fromStringInput(tuple.field1.getString(0)).toWKT();
                    return leftWkt + ";" + rightWkt;
                },
                (Class<Tuple2<Record, Record>>) (Class<?>) Tuple2.class,
                String.class
        );
        spatialJoin.connectTo(0, toWkt, 0);

        // TextFileSink (.wkt)
        String outputUri = new File(OUTPUT_FILE).toURI().toString();
        TextFileSink<String> sink = new TextFileSink<>(outputUri, String.class);
        toWkt.connectTo(0, sink, 0);

        WayangPlan wayangPlan = new WayangPlan(sink);

        // Explain: print logical and execution plans
        System.out.println("=== EXPLAIN: filtered_cross_pg_file_java ===");
        wayangContext.explain(wayangPlan);
        System.out.println("=== END EXPLAIN ===");

        wayangContext.execute("Benchmark filtered_cross_pg_file_java", wayangPlan);
        System.out.printf("[wayang] filtered_cross_pg_file_java output written to %s%n", OUTPUT_FILE);
    }
}
