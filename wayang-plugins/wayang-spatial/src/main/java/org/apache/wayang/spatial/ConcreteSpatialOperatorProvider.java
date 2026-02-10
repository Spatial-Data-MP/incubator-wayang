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

package org.apache.wayang.spatial;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.SpatialFilterDataQuantaBuilder;
import org.apache.wayang.api.SpatialJoinDataQuantaBuilder;
import org.apache.wayang.core.api.spatial.Geometry;
import org.apache.wayang.core.api.spatial.SpatialOperatorProvider;
import org.apache.wayang.core.api.spatial.SpatialPredicateType;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.spatial.data.WGeometry;
import org.apache.wayang.spatial.function.SpatialPredicate;

/**
 * Implementation of {@link SpatialOperatorProvider} that provides concrete
 * spatial operator builders using JTS/Sedona-backed implementations.
 */
public class ConcreteSpatialOperatorProvider implements SpatialOperatorProvider {

    @Override
    @SuppressWarnings("unchecked")
    public <In, Out> Object createSpatialFilterBuilder(
            Object inputBuilder,
            SerializableFunction<In, ? extends Geometry> keyUdf,
            SpatialPredicateType predicate,
            Geometry filterGeometry,
            Object planBuilder) {

        DataQuantaBuilder<?, In> input = (DataQuantaBuilder<?, In>) inputBuilder;
        JavaPlanBuilder javaPlanBuilder = (JavaPlanBuilder) planBuilder;

        // Convert the abstract Geometry to WGeometry
        WGeometry wGeometry = toWGeometry(filterGeometry);

        // Convert the key UDF to return WGeometry
        SerializableFunction<In, WGeometry> wKeyUdf = wrapKeyUdf(keyUdf);

        // Convert predicate type to concrete predicate
        SpatialPredicate spatialPredicate = SpatialPredicate.fromType(predicate);

        return new SpatialFilterDataQuantaBuilder<>(
                input,
                wKeyUdf,
                spatialPredicate,
                wGeometry,
                javaPlanBuilder
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public <In, Out> Object createSpatialFilterBuilder(
            Object inputBuilder,
            SerializableFunction<In, ? extends Geometry> keyUdf,
            SpatialPredicateType predicate,
            Geometry filterGeometry,
            String sqlGeometryColumn,
            Object planBuilder) {

        DataQuantaBuilder<?, In> input = (DataQuantaBuilder<?, In>) inputBuilder;
        JavaPlanBuilder javaPlanBuilder = (JavaPlanBuilder) planBuilder;

        // Convert the abstract Geometry to WGeometry
        WGeometry wGeometry = toWGeometry(filterGeometry);

        // Convert the key UDF to return WGeometry
        SerializableFunction<In, WGeometry> wKeyUdf = wrapKeyUdf(keyUdf);

        // Convert predicate type to concrete predicate
        SpatialPredicate spatialPredicate = SpatialPredicate.fromType(predicate);

        SpatialFilterDataQuantaBuilder<In> builder = new SpatialFilterDataQuantaBuilder<>(
                input,
                wKeyUdf,
                spatialPredicate,
                wGeometry,
                javaPlanBuilder
        );

        // Set the SQL geometry column name for database pushdown
        if (sqlGeometryColumn != null && !sqlGeometryColumn.isEmpty()) {
            builder.withSqlGeometryColumnName(sqlGeometryColumn);
        }

        return builder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <In0, In1> Object createSpatialJoinBuilder(
            Object inputBuilder0,
            Object inputBuilder1,
            SerializableFunction<In0, ? extends Geometry> keyUdf0,
            SerializableFunction<In1, ? extends Geometry> keyUdf1,
            SpatialPredicateType predicate,
            Object planBuilder) {

        DataQuantaBuilder<?, In0> input0 = (DataQuantaBuilder<?, In0>) inputBuilder0;
        DataQuantaBuilder<?, In1> input1 = (DataQuantaBuilder<?, In1>) inputBuilder1;
        JavaPlanBuilder javaPlanBuilder = (JavaPlanBuilder) planBuilder;

        // Convert the key UDFs to return WGeometry
        SerializableFunction<In0, WGeometry> wKeyUdf0 = wrapKeyUdf(keyUdf0);
        SerializableFunction<In1, WGeometry> wKeyUdf1 = wrapKeyUdf(keyUdf1);

        // Convert predicate type to concrete predicate
        SpatialPredicate spatialPredicate = SpatialPredicate.fromType(predicate);

        return new SpatialJoinDataQuantaBuilder<>(
                input0,
                input1,
                wKeyUdf0,
                wKeyUdf1,
                spatialPredicate,
                javaPlanBuilder
        );
    }

    /**
     * Converts an abstract Geometry to WGeometry.
     */
    private WGeometry toWGeometry(Geometry geometry) {
        if (geometry instanceof WGeometry) {
            return (WGeometry) geometry;
        }
        // Convert via WKT
        return WGeometry.fromStringInput(geometry.toWKT());
    }

    /**
     * Wraps a key UDF that returns Geometry to return WGeometry.
     */
    private <T> SerializableFunction<T, WGeometry> wrapKeyUdf(
            SerializableFunction<T, ? extends Geometry> keyUdf) {
        return input -> {
            Geometry geom = keyUdf.apply(input);
            if (geom instanceof WGeometry) {
                return (WGeometry) geom;
            }
            return WGeometry.fromStringInput(geom.toWKT());
        };
    }
}
