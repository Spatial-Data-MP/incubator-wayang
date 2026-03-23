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
import org.apache.wayang.spatial.data.WayangGeometry;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jol.info.GraphLayout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;

/**
 * Measures STRtree memory footprint at various scales using JOL (Java Object Layout).
 *
 * Replicates the exact insertion pattern from JavaSpatialJoinOperator:
 *   index.insert(geom.getEnvelopeInternal(), new SimpleEntry<>(record, geom))
 *
 * Usage:
 *   java -Xmx8g -cp "..." org.apache.wayang.spatial.integration.STRtreeMemoryProfile [wkt_file] [max_rows]
 *
 * Defaults: boxes_1M_0.001_1.wkt, measures at 1k, 10k, 100k, 500k, 1M rows.
 */
public class STRtreeMemoryProfile {

    private static final int[] CHECKPOINTS = {1_000, 10_000, 100_000, 500_000, 1_000_000};

    public static void main(String[] args) throws IOException {
        String wktFile = args.length > 0 ? args[0]
                : "wayang-plugins/wayang-spatial/boxes_1M_0.001_1.wkt";
        int maxRows = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;

        System.out.println("STRtree Memory Profile");
        System.out.println("======================");
        System.out.println("WKT file: " + wktFile);
        System.out.println("Max rows: " + maxRows);
        System.out.println();

        // --- Method 1: Runtime.freeMemory (coarse) ---
        System.out.println("--- Method 1: Runtime heap estimate ---");
        System.out.printf("%-12s %15s %15s %15s%n",
                "Rows", "Heap used (MB)", "Per entry (B)", "Build time (ms)");
        System.out.println("-".repeat(60));

        Runtime rt = Runtime.getRuntime();

        for (int checkpoint : CHECKPOINTS) {
            if (checkpoint > maxRows) break;

            // Force GC before measurement
            System.gc();
            Thread.yield();
            System.gc();
            long heapBefore = rt.totalMemory() - rt.freeMemory();

            STRtree tree = new STRtree();
            int count = 0;
            long buildStart = System.nanoTime();

            try (BufferedReader reader = new BufferedReader(new FileReader(wktFile))) {
                String line;
                while ((line = reader.readLine()) != null && count < checkpoint) {
                    Record record = new Record(new Object[]{line});
                    WayangGeometry wGeom = WayangGeometry.fromStringInput(line);
                    Geometry geom = wGeom.getGeometry();
                    if (geom != null) {
                        tree.insert(geom.getEnvelopeInternal(),
                                new AbstractMap.SimpleEntry<>(record, geom));
                    }
                    count++;
                }
            }

            tree.build();
            long buildMs = (System.nanoTime() - buildStart) / 1_000_000;

            System.gc();
            Thread.yield();
            System.gc();
            long heapAfter = rt.totalMemory() - rt.freeMemory();

            double heapMB = (heapAfter - heapBefore) / 1_000_000.0;
            double perEntry = (heapAfter - heapBefore) / (double) count;

            System.out.printf("%-12d %15.1f %15.0f %15d%n",
                    count, heapMB, perEntry, buildMs);
        }

        // --- Method 2: JOL deep graph analysis (precise, but slow for large trees) ---
        System.out.println();
        System.out.println("--- Method 2: JOL deep graph analysis ---");

        // JOL is expensive on large graphs, so limit to smaller sizes
        int[] jolCheckpoints = {1_000, 10_000, 100_000};

        System.out.printf("%-12s %15s %15s %15s %15s%n",
                "Rows", "Total (MB)", "Per entry (B)", "Objects", "Classes");
        System.out.println("-".repeat(75));

        for (int checkpoint : jolCheckpoints) {
            if (checkpoint > maxRows) break;

            STRtree tree = new STRtree();
            int count = 0;

            try (BufferedReader reader = new BufferedReader(new FileReader(wktFile))) {
                String line;
                while ((line = reader.readLine()) != null && count < checkpoint) {
                    Record record = new Record(new Object[]{line});
                    WayangGeometry wGeom = WayangGeometry.fromStringInput(line);
                    Geometry geom = wGeom.getGeometry();
                    if (geom != null) {
                        tree.insert(geom.getEnvelopeInternal(),
                                new AbstractMap.SimpleEntry<>(record, geom));
                    }
                    count++;
                }
            }

            tree.build();

            System.out.printf("  Analyzing %d entries with JOL (this may take a moment)...%n", count);
            GraphLayout layout = GraphLayout.parseInstance(tree);

            double totalMB = layout.totalSize() / 1_000_000.0;
            double perEntry = layout.totalSize() / (double) count;
            long totalObjects = layout.totalCount();

            System.out.printf("%-12d %15.1f %15.0f %15d %15s%n",
                    count, totalMB, perEntry, totalObjects, "—");

            // Print class breakdown for smallest size
            if (checkpoint == 1_000) {
                System.out.println();
                System.out.println("  Class breakdown (1k entries):");
                System.out.println(layout.toFootprint());
            }
        }

        // --- Single entry analysis ---
        System.out.println();
        System.out.println("--- Single entry component sizes (JOL) ---");

        String sampleWkt = "POLYGON((0.5 0.5, 0.501 0.5, 0.501 0.501, 0.5 0.501, 0.5 0.5))";
        Record sampleRecord = new Record(new Object[]{sampleWkt});
        WayangGeometry sampleWGeom = WayangGeometry.fromStringInput(sampleWkt);
        Geometry sampleGeom = sampleWGeom.getGeometry();
        AbstractMap.SimpleEntry<Record, Geometry> sampleEntry =
                new AbstractMap.SimpleEntry<>(sampleRecord, sampleGeom);

        System.out.printf("  Record (1 field):        %d bytes%n",
                GraphLayout.parseInstance(sampleRecord).totalSize());
        System.out.printf("  WayangGeometry:          %d bytes%n",
                GraphLayout.parseInstance(sampleWGeom).totalSize());
        System.out.printf("  JTS Geometry (Polygon):  %d bytes%n",
                GraphLayout.parseInstance(sampleGeom).totalSize());
        System.out.printf("  Envelope:                %d bytes%n",
                GraphLayout.parseInstance(sampleGeom.getEnvelopeInternal()).totalSize());
        System.out.printf("  SimpleEntry<Rec,Geom>:   %d bytes%n",
                GraphLayout.parseInstance(sampleEntry).totalSize());
        System.out.printf("  WKT string (%d chars):   %d bytes%n",
                sampleWkt.length(),
                GraphLayout.parseInstance(sampleWkt).totalSize());
    }
}
