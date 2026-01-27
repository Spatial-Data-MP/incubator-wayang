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

package org.apache.wayang.core.api.spatial;

import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

/**
 * SPI interface for spatial operator implementations.
 * Implemented by the wayang-spatial plugin to provide concrete spatial operations.
 */
public interface SpatialOperatorProvider {

    /**
     * Creates a spatial filter builder for filtering data based on spatial predicates.
     *
     * @param inputBuilder   the input DataQuantaBuilder (passed as Object to avoid circular dependencies)
     * @param keyUdf         function to extract geometry from input elements
     * @param predicate      the spatial predicate type to apply
     * @param filterGeometry the geometry to filter against
     * @param planBuilder    the JavaPlanBuilder (passed as Object)
     * @param <In>           the input element type
     * @param <Out>          the output element type (same as In for filters)
     * @return a DataQuantaBuilder wrapping the spatial filter (as Object)
     */
    <In, Out> Object createSpatialFilterBuilder(
            Object inputBuilder,
            SerializableFunction<In, ? extends Geometry> keyUdf,
            SpatialPredicateType predicate,
            Geometry filterGeometry,
            Object planBuilder
    );

    /**
     * Creates a spatial filter builder for filtering data based on spatial predicates,
     * with SQL pushdown support.
     *
     * @param inputBuilder      the input DataQuantaBuilder (passed as Object to avoid circular dependencies)
     * @param keyUdf            function to extract geometry from input elements
     * @param predicate         the spatial predicate type to apply
     * @param filterGeometry    the geometry to filter against
     * @param sqlGeometryColumn the name of the geometry column in the database for SQL pushdown
     * @param planBuilder       the JavaPlanBuilder (passed as Object)
     * @param <In>              the input element type
     * @param <Out>             the output element type (same as In for filters)
     * @return a DataQuantaBuilder wrapping the spatial filter (as Object)
     */
    <In, Out> Object createSpatialFilterBuilder(
            Object inputBuilder,
            SerializableFunction<In, ? extends Geometry> keyUdf,
            SpatialPredicateType predicate,
            Geometry filterGeometry,
            String sqlGeometryColumn,
            Object planBuilder
    );

    /**
     * Creates a spatial join builder for joining two data sources based on spatial predicates.
     *
     * @param inputBuilder0 the first input DataQuantaBuilder (passed as Object)
     * @param inputBuilder1 the second input DataQuantaBuilder (passed as Object)
     * @param keyUdf0       function to extract geometry from first input elements
     * @param keyUdf1       function to extract geometry from second input elements
     * @param predicate     the spatial predicate type to apply
     * @param planBuilder   the JavaPlanBuilder (passed as Object)
     * @param <In0>         the first input element type
     * @param <In1>         the second input element type
     * @return a DataQuantaBuilder wrapping the spatial join (as Object)
     */
    <In0, In1> Object createSpatialJoinBuilder(
            Object inputBuilder0,
            Object inputBuilder1,
            SerializableFunction<In0, ? extends Geometry> keyUdf0,
            SerializableFunction<In1, ? extends Geometry> keyUdf1,
            SpatialPredicateType predicate,
            Object planBuilder
    );
}
