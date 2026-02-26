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

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.UnarySourceDataQuantaBuilder;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spark.Spark;

import java.util.Arrays;
import java.util.Collection;

public class SpatialJoin {

    public static void main(String[] args) {

        System.out.println(Arrays.toString(args));

        if (args.length <= 3) {
            System.err.print("Missing Paths: <input file1 URL> <input file2 URL> <platform>");
            System.exit(1);
        }

        WayangContext wayangContext = new WayangContext(new Configuration());

        String platform = args[2];
        switch (platform) {
            case "java":
                wayangContext.withPlugin(Java.basicPlugin());
                wayangContext.withPlugin(Spatial.javaPlugin());
                break;
            case "spark":
                wayangContext.withPlugin(Spark.basicPlugin());
                wayangContext.withPlugin(Spatial.sparkPlugin());
                break;
            default:
                wayangContext.withPlugin(Java.basicPlugin());
                wayangContext.withPlugin(Spark.basicPlugin());
                wayangContext.withPlugin(Spatial.plugin());

        }

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(SpatialJoin.class);


        String file1Url = args[1];
        String file2Url = args[2];
        DataQuantaBuilder<UnarySourceDataQuantaBuilder<?, String>, String> table1 = planBuilder.readTextFile(file1Url);
        DataQuantaBuilder<UnarySourceDataQuantaBuilder<?, String>, String> table2 = planBuilder.readTextFile(file2Url);




        DataQuantaBuilder<?, ?> joinResult = table1
                .<String>spatialJoin(
                        (line -> WayangGeometry.fromStringInput(line.split("\",")[0].replace("\"", ""))),
                        table2,
                        (line -> WayangGeometry.fromStringInput(line.split("\",")[0].replace("\"", ""))),
                        SpatialPredicate.INTERSECTS
                );
        joinResult.withTargetPlatform(Spark.platform());

        Collection<Long> outputcount = joinResult
                .count()
                .withTargetPlatform(Spark.platform())
                .collect();
        System.out.println("Spatial Join (intersects): " + outputcount);
    }
}
