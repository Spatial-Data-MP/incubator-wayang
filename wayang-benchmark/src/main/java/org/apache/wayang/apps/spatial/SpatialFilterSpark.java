/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.spatial;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.spark.Spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class SpatialFilterSpark {

    public static void main(String[] args) {

        WayangContext wayangContext = new WayangContext()
                .withPlugin(Spark.basicPlugin())
//                .with(Java.basicPlugin())
                ;
        // Set up WayangContext.
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 10);

        // Execute the job: keep only even numbers.
        final Collection<Integer> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .filter(i -> (i & 1) == 0).withName("Filter even numbers")
                .withUdfJarOf(SpatialFilterSpark.class)
                .collect();


        // Print output
        for (Integer t : outputValues) {
            System.out.println(t.toString());
        }

        /*

        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext()
//                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                ;

        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Input polygons: nested axis-aligned squares.
        final List<WGeometry> inputValues = Arrays.asList(
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.30 0.00,0.30 0.30,0.00 0.30,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.20 0.00,0.20 0.20,0.00 0.20,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.10 0.00,0.10 0.10,0.00 0.10,0.00 0.00))")
        );

        // Query geometry: a square entirely inside the 0.4 and 0.3 squares,
        // but outside the 0.2 and 0.1 squares.
        WGeometry queryGeometry = WGeometry.fromStringInput(
                "POLYGON((0.25 0.25,0.35 0.25,0.35 0.35,0.25 0.35,0.25 0.25))"
        );

        final Collection<WGeometry> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .spatialFilter(
                        (input -> (WGeometry) input),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                ).withName("Spatial filter (INTERSECTS)")
                .withUdfJarOf(CustomerTransactionHybridJoin.class)
                .collect();

        // We expect only the first two polygons to intersect the query geometry.
        Set<WGeometry> expectedOutput = WayangCollections.asSet(
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))"),
                WGeometry.fromStringInput("POLYGON((0.00 0.00,0.30 0.00,0.30 0.30,0.00 0.30,0.00 0.00))")
        );
        // Print output
        for (WGeometry t : outputValues) {
            System.out.println(t.toString());
        }
        */




//        assertEquals(expectedOutput, WayangCollections.asSet(outputValues));

        /*

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/mydb");
        configuration.setProperty("wayang.postgres.jdbc.user", "zoi");

        // Create Wayang context
        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        // Plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("CustomerTransactionChurn")
                .withUdfJarOf(CustomerTransactionHybridJoin.class);

        // Read transactions from PostgreSQL
        DataQuantaBuilder<?, Record> transactions =
                planBuilder.readTable(new PostgresTableSource("transactions"))
                        .filter(tuple -> (Double) tuple.getField(2) > 1000);

        Path path = Paths.get("src/main/resources/input/customers.csv").toAbsolutePath();

        DataQuantaBuilder<?, Record> customers = planBuilder
                // Read customers from csv file
                .readTextFile("file:" + path.toUri().getPath())

                // Map customers to Record(customerId, name, location)
                .map(line -> {
                    String[] cols = line.split(",");
                    return new Record(Integer.parseInt(cols[0]), cols[1], cols[2]);
                });


        // Join customers with transactions on customerId
        Collection<Tuple2<Record, Record>> joined = customers
                .join(  customerRecord -> customerRecord.getInt(0), // customer.id
                        transactions,
                        transactionsRecord -> transactionsRecord.getInt(1) // transaction.customerId
                )
                .collect();

        // Print output
        for (Tuple2<Record, Record> t : joined) {
            System.out.println(t);
        }
        */
    }
}