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
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicateType;
import org.apache.wayang.java.Java;
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spark.Spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class SpatialFilter {
    public static void main(String[] args) {
        System.out.println(Arrays.toString((args)));

        if (args.length <= 3) {
            System.err.print("Usage:");
        }

        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("filter test")
                .withUdfJarOf(SpatialFilter.class);

        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
//                "POLYGON((-84.07287597656251 37.16644514778088, -81.79870605468751 37.16644514778088, -81.79870605468751 38.15788469869244, -84.07287597656251 38.15788469869244, -84.07287597656251 37.16644514778088))"
                "POLYGON((12.777099609375 52.219050335542484, 13.991088867187502 52.219050335542484, 13.991088867187502 52.71766191466581, 12.777099609375 52.71766191466581, 12.777099609375 52.219050335542484))"
//                "POLYGON((13.054504394531252 52.305791671751265, 13.23577880859375 52.33433208908722, 13.342895507812502 52.359499525558654, 13.521423339843752 52.37459311076614, 13.609313964843752 52.33433208908722, 13.669738769531252 52.320903597434054, 13.746643066406252 52.371239426380214, 13.787841796875002 52.40476481199653, 13.807067871093752 52.44830975509531, 13.807067871093752 52.48679443193377, 13.675231933593752 52.503516406073174, 13.686218261718752 52.54028236828442, 13.634033203125002 52.58035560366049, 13.537902832031252 52.612054291512536, 13.54339599609375 52.66372397759699, 13.47198486328125 52.69536233532457, 13.430786132812502 52.67871342471301, 13.359375000000002 52.645396558286066, 13.31817626953125 52.67371751370322, 13.24676513671875 52.67871342471301, 13.191833496093752 52.64872938781106, 13.16986083984375 52.612054291512536, 13.114929199218752 52.612054291512536, 13.09295654296875 52.57201003157308, 13.09295654296875 52.54529352469354, 13.08197021484375 52.50853175834131, 13.136901855468752 52.50853175834131, 13.065490722656252 52.473412273757006, 13.08197021484375 52.44663574493768, 13.046264648437502 52.40308914740344, 13.065490722656252 52.362854101276355, 13.054504394531252 52.305791671751265))"
        );

        String fileUrl = args[1];
        String platform = args[2];

        Collection<Long> outputcount =
                planBuilder.readTextFile(fileUrl)
                        .spatialFilter(
                                (input -> WayangGeometry.fromStringInput((input.split("\",")[0]).replace("\"", ""))),
                                SpatialPredicateType.INTERSECTS,
                                queryGeometry
                        ).withTargetPlatform(Java.platform())
                        .count()
                        .collect();

        System.out.println("Spatial Filter (INTERSECTS: " + outputcount);

    }
}

/*
public class SpatialFilter {

    public static void main(String[] args) {

        WayangContext wayangContext = new WayangContext()
//                .withPlugin(Spark.basicPlugin())
                .with(Java.basicPlugin())
                ;
        // Set up WayangContext.
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 10);

        // Execute the job: keep only even numbers.
        final Collection<Integer> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .filter(i -> (i & 1) == 0).withName("Filter even numbers")
                .withUdfJarOf(SpatialFilter.class)
                .collect();


        // Print output
        for (Integer t : outputValues) {
            System.out.println(t.toString());
        }*/

        /*

        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext()
//                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                ;

        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Input polygons: nested axis-aligned squares.
        final List<WayangGeometry> inputValues = Arrays.asList(
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.30 0.00,0.30 0.30,0.00 0.30,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.20 0.00,0.20 0.20,0.00 0.20,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.10 0.00,0.10 0.10,0.00 0.10,0.00 0.00))")
        );

        // Query geometry: a square entirely inside the 0.4 and 0.3 squares,
        // but outside the 0.2 and 0.1 squares.
        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0.25 0.25,0.35 0.25,0.35 0.35,0.25 0.35,0.25 0.25))"
        );

        final Collection<WayangGeometry> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .spatialFilter(
                        (input -> (WayangGeometry) input),
                        SpatialPredicateType.INTERSECTS,
                        queryGeometry
                ).withName("Spatial filter (INTERSECTS)")
                .withUdfJarOf(CustomerTransactionHybridJoin.class)
                .collect();

        // We expect only the first two polygons to intersect the query geometry.
        Set<WayangGeometry> expectedOutput = WayangCollections.asSet(
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.30 0.00,0.30 0.30,0.00 0.30,0.00 0.00))")
        );
        // Print output
        for (WayangGeometry t : outputValues) {
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
//    }
//}
