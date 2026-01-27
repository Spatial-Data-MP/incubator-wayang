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

/**
 * Factory for creating spatial operators.
 * Delegates to a registered {@link SpatialOperatorProvider}.
 * The provider is registered by the wayang-spatial plugin at class loading time.
 */
public class SpatialOperatorFactory {

    private static volatile SpatialOperatorProvider provider = null;

    /**
     * Registers a spatial operator provider.
     * This is typically called by the wayang-spatial plugin during initialization.
     *
     * @param p the provider to register
     */
    public static void registerProvider(SpatialOperatorProvider p) {
        provider = p;
    }

    /**
     * Gets the registered spatial operator provider.
     *
     * @return the registered provider
     * @throws IllegalStateException if no provider is registered
     */
    public static SpatialOperatorProvider getProvider() {
        if (provider == null) {
            throw new IllegalStateException(
                    "Spatial plugin not loaded. Add wayang-spatial dependency " +
                    "and call Spatial.plugin() or Spatial.javaPlugin() to register with WayangContext.");
        }
        return provider;
    }

    /**
     * Checks if a spatial operator provider is available.
     *
     * @return true if a provider is registered, false otherwise
     */
    public static boolean isAvailable() {
        return provider != null;
    }
}
