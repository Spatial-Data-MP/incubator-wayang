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

package org.apache.wayang.spatial.api;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.SpatialFilterDataQuantaBuilder;
import org.apache.wayang.api.SpatialJoinDataQuantaBuilder;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.spatial.data.WGeometry;
import org.apache.wayang.spatial.function.SpatialPredicate;

/**
 * Java-friendly static methods for spatial operations.
 *
 * Usage:
 * <pre>{@code
 * import static org.apache.wayang.spatial.api.SpatialOps.*;
 *
 * // Spatial filter
 * spatialFilter(planBuilder, inputBuilder, keyExtractor, predicate, filterGeometry)
 *     .count()
 *     .collect();
 *
 * // Spatial join
 * spatialJoin(planBuilder, left, leftKey, right, rightKey, predicate)
 *     .count()
 *     .collect();
 * }</pre>
 */
public final class SpatialOps {

    private SpatialOps() {
        // Utility class
    }

    /**
     * Apply a spatial filter to the input DataQuantaBuilder.
     *
     * @param planBuilder the JavaPlanBuilder
     * @param input the input DataQuantaBuilder
     * @param keyExtractor function to extract WGeometry from elements
     * @param predicate the spatial predicate
     * @param filterGeometry the geometry to filter against
     * @param <T> the element type
     * @return a SpatialFilterDataQuantaBuilder for further chaining
     */
    public static <T> SpatialFilterDataQuantaBuilder<T> spatialFilter(
            JavaPlanBuilder planBuilder,
            DataQuantaBuilder<?, T> input,
            SerializableFunction<T, WGeometry> keyExtractor,
            SpatialPredicate predicate,
            WGeometry filterGeometry) {
        return new SpatialFilterDataQuantaBuilder<>(input, keyExtractor, predicate, filterGeometry, planBuilder);
    }

    /**
     * Apply a spatial join between two DataQuantaBuilders.
     *
     * @param planBuilder the JavaPlanBuilder
     * @param left the left input
     * @param leftKeyExtractor function to extract WGeometry from left elements
     * @param right the right input
     * @param rightKeyExtractor function to extract WGeometry from right elements
     * @param predicate the spatial predicate
     * @param <L> the left element type
     * @param <R> the right element type
     * @return a SpatialJoinDataQuantaBuilder for further chaining
     */
    public static <L, R> SpatialJoinDataQuantaBuilder<L, R> spatialJoin(
            JavaPlanBuilder planBuilder,
            DataQuantaBuilder<?, L> left,
            SerializableFunction<L, WGeometry> leftKeyExtractor,
            DataQuantaBuilder<?, R> right,
            SerializableFunction<R, WGeometry> rightKeyExtractor,
            SpatialPredicate predicate) {
        return new SpatialJoinDataQuantaBuilder<>(left, right, leftKeyExtractor, rightKeyExtractor, predicate, planBuilder);
    }
}
