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

package org.apache.wayang.applications;

import org.apache.wayang.api.*;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.spatial.data.WGeometry;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicateType;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;
import org.postgresql.core.v3.QueryExecutorImpl;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

public class TestSpatialOperators {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TestSpatialOperators.class);

//    public static void main(String[] args){

    @Test
    void testSpatialOperators() {
        //// Debugging might be useful, set level to "FINEST" to see actual db query strings
        Logger logger = Logger.getLogger(QueryExecutorImpl.class.getName());
        ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(Level.FINEST);
//        handler.setFilter(record -> record.getMessage() != null && record.getMessage().contains("query="));
//        logger.addHandler(handler);
//        logger.setUseParentHandlers(false);
//        logger.setLevel(Level.FINEST);

        System.out.println( ">>> Test a Filter Operator");

        //// Db Connection, local db credentials!
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/spatialdb"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        // Set up WayangContext.
        //org.apache.wayang.api.MultiContext wayang = new MultiContext(configuration).withPlugin(Java.basicPlugin()).withPlugin(Postgres.plugin());

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spark.basicPlugin())
                ;
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(TestSpatialOperators.class);

        WGeometry queryGeometry = WGeometry.fromStringInput(
//                "POLYGON((-84.07287597656251 37.16644514778088, -81.79870605468751 37.16644514778088, -81.79870605468751 38.15788469869244, -84.07287597656251 38.15788469869244, -84.07287597656251 37.16644514778088))"
                "POLYGON((12.777099609375 52.219050335542484, 13.991088867187502 52.219050335542484, 13.991088867187502 52.71766191466581, 12.777099609375 52.71766191466581, 12.777099609375 52.219050335542484))"
//                "POLYGON((13.054504394531252 52.305791671751265, 13.23577880859375 52.33433208908722, 13.342895507812502 52.359499525558654, 13.521423339843752 52.37459311076614, 13.609313964843752 52.33433208908722, 13.669738769531252 52.320903597434054, 13.746643066406252 52.371239426380214, 13.787841796875002 52.40476481199653, 13.807067871093752 52.44830975509531, 13.807067871093752 52.48679443193377, 13.675231933593752 52.503516406073174, 13.686218261718752 52.54028236828442, 13.634033203125002 52.58035560366049, 13.537902832031252 52.612054291512536, 13.54339599609375 52.66372397759699, 13.47198486328125 52.69536233532457, 13.430786132812502 52.67871342471301, 13.359375000000002 52.645396558286066, 13.31817626953125 52.67371751370322, 13.24676513671875 52.67871342471301, 13.191833496093752 52.64872938781106, 13.16986083984375 52.612054291512536, 13.114929199218752 52.612054291512536, 13.09295654296875 52.57201003157308, 13.09295654296875 52.54529352469354, 13.08197021484375 52.50853175834131, 13.136901855468752 52.50853175834131, 13.065490722656252 52.473412273757006, 13.08197021484375 52.44663574493768, 13.046264648437502 52.40308914740344, 13.065490722656252 52.362854101276355, 13.054504394531252 52.305791671751265))"
        );

        final Collection<Long> outputValues = planBuilder
                .readTextFile("/incubator-wayang/wayang-applications/src/main/java/org/apache/wayang/applications/OSM2015_parks.csv")
                .spatialFilter(
                        (input -> WGeometry.fromStringInput((input.split("\",")[0]).replace("\"", ""))),
                        SpatialPredicateType.INTERSECTS,
                        queryGeometry
                ).withTargetPlatform(Java.platform())
                .withName("Spatial Filter (intersects)")
                .count()
                .collect();

        System.out.println("Spatial Filter (intersects): " + outputValues);

        if (1+1 == 2) {
            return;
        }

        System.out.println( "*** Done. ***" );
    }


    @Test
    void testSpatialJoin() {
        Logger logger = Logger.getLogger(QueryExecutorImpl.class.getName());
        ConsoleHandler handler = new ConsoleHandler();

        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spark.basicPlugin())
//                .withPlugin(Spatial.plugin)
                ;

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(TestSpatialOperators.class);


        DataQuantaBuilder<UnarySourceDataQuantaBuilder<?, String>, String> table1 = planBuilder.readTextFile("/incubator-wayang/wayang-applications/src/main/java/org/apache/wayang/applications/OSM2015_parks.csv");
        DataQuantaBuilder<UnarySourceDataQuantaBuilder<?, String>, String> table2 = planBuilder.readTextFile("/incubator-wayang/wayang-applications/src/main/java/org/apache/wayang/applications/OSM2015_sports(1).csv");


        WGeometry queryGeometry = WGeometry.fromStringInput(
//                "POLYGON((-84.07287597656251 37.16644514778088, -81.79870605468751 37.16644514778088, -81.79870605468751 38.15788469869244, -84.07287597656251 38.15788469869244, -84.07287597656251 37.16644514778088))"
                "POLYGON((12.777099609375 52.219050335542484, 13.991088867187502 52.219050335542484, 13.991088867187502 52.71766191466581, 12.777099609375 52.71766191466581, 12.777099609375 52.219050335542484))"
//                "POLYGON((13.054504394531252 52.305791671751265, 13.23577880859375 52.33433208908722, 13.342895507812502 52.359499525558654, 13.521423339843752 52.37459311076614, 13.609313964843752 52.33433208908722, 13.669738769531252 52.320903597434054, 13.746643066406252 52.371239426380214, 13.787841796875002 52.40476481199653, 13.807067871093752 52.44830975509531, 13.807067871093752 52.48679443193377, 13.675231933593752 52.503516406073174, 13.686218261718752 52.54028236828442, 13.634033203125002 52.58035560366049, 13.537902832031252 52.612054291512536, 13.54339599609375 52.66372397759699, 13.47198486328125 52.69536233532457, 13.430786132812502 52.67871342471301, 13.359375000000002 52.645396558286066, 13.31817626953125 52.67371751370322, 13.24676513671875 52.67871342471301, 13.191833496093752 52.64872938781106, 13.16986083984375 52.612054291512536, 13.114929199218752 52.612054291512536, 13.09295654296875 52.57201003157308, 13.09295654296875 52.54529352469354, 13.08197021484375 52.50853175834131, 13.136901855468752 52.50853175834131, 13.065490722656252 52.473412273757006, 13.08197021484375 52.44663574493768, 13.046264648437502 52.40308914740344, 13.065490722656252 52.362854101276355, 13.054504394531252 52.305791671751265))"
        );

        SpatialFilterDataQuantaBuilder<String> filteredTable1 = table1.spatialFilter(
                (input -> WGeometry.fromStringInput((input.split("\",")[0]).replace("\"", ""))),
                SpatialPredicateType.INTERSECTS,
                queryGeometry
        );

        SpatialFilterDataQuantaBuilder<String> filteredTable2 = table2.spatialFilter(
                (input -> WGeometry.fromStringInput((input.split("\",")[0]).replace("\"", ""))),
                SpatialPredicateType.INTERSECTS,
                queryGeometry
        );

        Collection<Long> outputcount = filteredTable1
                .<String>spatialJoin(
                        (line -> WGeometry.fromStringInput(line.split("\",")[0].replace("\"", ""))),
                        table2,
                        (line -> WGeometry.fromStringInput(line.split("\",")[0].replace("\"", ""))),
                        SpatialPredicateType.INTERSECTS
                )
                .withTargetPlatform(Spark.platform())
                .count()
                .collect();
//                .collect();
//                .map(pair -> pair.field0 + "\n" + pair.field1)//left.getWKT() + "\n" + right.getWKT();
//                .collect();
//                .writeTextFile("/incubator-wayang/wayang-applications/src/main/java/org/apache/wayang/applications/output.txt", input -> input, "join results");
        System.out.println("Spatial Filter (intersects): " + outputcount);
    }


    private static void printResults(int n, Collection<Record> result) {
        // print up to n records
        int count = 0;
        Iterator<Record> iterator = result.iterator();
        while (iterator.hasNext() && count++ < n) {
            Record record = iterator.next();
            System.out.print(" | ");
            for (int i = 0; i < record.size(); i++) {
                Object val = record.getField(i);
                if (val == null) { System.out.print(" " + " | "); }
                else System.out.print(val.toString() + " | ");
            }
            System.out.println("");
        }
    }
}
