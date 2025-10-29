package org.apache.wayang.applications;

import org.apache.wayang.api.*;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFilter;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.postgresql.core.v3.QueryExecutorImpl;
import org.postgresql.util.PGobject;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestSpatialOperators {

    public static void main(String[] args){

        //// Debugging might be useful, set level to "FINEST" to see actual db query strings
        Logger logger = Logger.getLogger(QueryExecutorImpl.class.getName());
        ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(Level.FINEST);
        // handler.setFilter(record -> record.getMessage() != null && record.getMessage().contains("query="));
//        logger.addHandler(handler);
//        logger.setUseParentHandlers(false);
//        logger.setLevel(Level.FINEST);

        System.out.println( ">>> Test a Filter Operator");

        //// Db Connection, local db credentials!
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5433/postgres"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "1234");

        // Set up WayangContext.
        //org.apache.wayang.api.MultiContext wayang = new MultiContext(configuration).withPlugin(Java.basicPlugin()).withPlugin(Postgres.plugin());

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(TestSpatialOperators.class);

//        Collection<Record> cars = planBuilder
//                // Read the text file.
//                .readTable(new PostgresTableSource("cars","name", "year"))
//                .filter(car -> car.getInt(1) >= 2015)
//                .asRecords()
//                .collect();

        /*final Collection<String> words = planBuilder
                .readTextFile("file:///Users/maximilianspeer/wayang/incubator-wayang/README.md")
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .collect();*/

        // *****   Within    *****  //
        final MapDataQuantaBuilder<Record, Integer> spiderWithin =
                    planBuilder
                    .readTable(new PostgresTableSource("spider", "id", "geom"))
                    .filter(t -> (Integer) t.getField(0) == 9).withSqlUdf("ST_Within(spider.geom, ST_MakeEnvelope(0.30, 0.00, 0.00, 0.30, 4326))")
                            .withTargetPlatform(Postgres.platform())
//                            .withTargetPlatform(Java.platform())
                    .filter(t -> (Integer) t.getField(0) <= 20)
                    .map(record -> (Integer) record.getField(0));
//                    .build().explain(true);
        System.out.println("Spider ST_Within: " + spiderWithin.collect().toString());


        // *****   Within Spatial Concept    *****  //
//        final FilterDataQuantaBuilder<Geometry> spiderWithinSpatial =
//                planBuilder
//                        .readTable(new PostgisTableSource("spider", "id", "geom")) : GeometryDataQUantyBuilder
//                        .filter(t -> ((Integer) t.getInt(0)) <= 20).withSqlUdf("id <= 20")
//                        .map(t -> { // Convert Types
//                            try {
//                                return (new WKBReader()).read(
//                                        DatatypeConverter.parseHexBinary(
//                                                ((PGobject) t.getField(1)).getValue())
//                                );
//                            } catch (ParseException e) {
//                                throw new RuntimeException(e);
//                            }
//                        })
//                        .filter(t -> t.getEnvelopeInternal().intersects(new Envelope(0.00, 0.30, 0.00, 0.30)))
//                        .filter(GeometryFilter::filter)
//                ;


        // *****   Within Spatial Concept    *****  //
        final SpatialFilterDataQuantaBuilder<Record> spiderWithinSpatial =
                planBuilder
                        .readTable(new PostgresTableSource("spider", "id", "geom"))
                .spatialFilter(t -> ((Geometry) t.getGeometry(1)).getEnvelopeInternal().intersects(new Envelope(0.00, 0.30, 0.00, 0.30)))
//                        .withSqlUdf("id <= 20")
                        .withTargetPlatform(Java.platform())
        ;
        System.out.println("InputValues from Collection: " + spiderWithinSpatial.collect().toString());

        if (1+1 == 2) {
            return;
        }


//                        .withSqlUdf("ST_Within(spider.geom, ST_MakeEnvelope(0.00, 0.30, 0.00, 0.30, 4326))").withTargetPlatform(Postgres.platform())
//                        .filter(t -> (Integer) t.getField(0) <= 20)
//                        .map(record -> (Integer) record.getField(0));
//                    .build().explain(true);
//        System.out.println("Spider ST_Within: " + spiderWithin.collect().toString());


//        final FilterDataQuantaBuilder<Geometry> spiderWithinSpatial =
//                planBuilder
//                        .readTable(new PostgisTableSource("spider", "id", "geom")) : GeometryDataQUantyBuilder
//                .filter(t -> ((Integer) t.getInt(0)) <= 20).withSqlUdf("id <= 20")
//                .map(t -> { // Convert Types
//                    try {
//                        return (new WKBReader()).read(
//                                DatatypeConverter.parseHexBinary(
//                                        ((PGobject) t.getField(1)).getValue())
//                        );
//                    } catch (ParseException e) {
//                        throw new RuntimeException(e);
//                    }
//                })
//                .filter(t -> t.getEnvelopeInternal().intersects(new Envelope(0.00, 0.30, 0.00, 0.30)))
//        ;

        //// Sample local Collection
        final List<Tuple2<String, Integer>> inputValues2 = Arrays.asList(
                new Tuple2<>("Yellow", 1)
                ,new Tuple2<>("Green", 2)
//                ,new Tuple2<>("Blue", 3)
        );
        final FilterDataQuantaBuilder<Tuple2<String, Integer>> dataQuanta1 = planBuilder
                .loadCollection(inputValues2)
                .filter(t -> true)
//                .filter(t -> t.getField0().equals("Yellow"))
                ;
//        System.out.println("InputValues from Collection: " + dataQuanta1.collect().toString());

        //// Polygons from db
        final FilterDataQuantaBuilder<Record> dataQuanta2
                = planBuilder.readTable(new PostgresTableSource("my_polygon", "id", "geom"))
                .filter(t -> true)
                ;
//        System.out.println("Polygons from DB: " + dataQuanta2.collect().toString());


        //// Non-Spatial Join operator
        final Collection<Tuple2<Tuple2<String, Integer>, Record>> outputValues =
                             dataQuanta1
                .join(Tuple2::getField1, dataQuanta2, (record -> record.getField(0)))
                .collect();
        System.out.println(outputValues.toString());


        // *****   All    *****  //
//        final Collection<Integer> outputValues =
//                planBuilder
//                .readTable(new PostgresTableSource("spider", "id", "geom"))
//                //.filter(r -> (Integer) r.getField(1) >= 18).withSqlUdf("year >= 18").withTargetPlatform(Java.platform())
//                // .asRecords()
//                //.projectRecords(new String[]{"name"})
//                .map(record -> (Integer) record.getField(0))
//                //.build().explain(false);
//                .collect();

//        System.out.println(outputValues.toString());
        /*
        SqlContext sqlContext = null;
        try {
            sqlContext = new SqlContext(configuration);

            Collection<Record> result = sqlContext.executeSql(
                    "select * from cars"
            );
            printResults(10, result);


        } catch (SQLException | SqlParseException e) {
            throw new RuntimeException(e);
        }



/*
        // Get a plan builder.
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
        //        .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(TestSpatialOperators.class);



        // Start building the WayangPlan.
        Collection<Record> cars = planBuilder
                // Read the text file.
                .readTable(new PostgresTableSource("cars","name", "year"))
                .filter(car -> car.getInt(1) >= 2015)

                .collect();

                // Split each line by non-word characters.
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(10, 100, 0.9)
                .withName("Split words")

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                // Attach counter to each word.
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                // Execute the plan and collect the results.
                .collect();
*/
        System.out.println( "*** Done. ***" );
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
