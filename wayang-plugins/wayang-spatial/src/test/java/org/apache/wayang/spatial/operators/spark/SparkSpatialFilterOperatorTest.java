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

package org.apache.wayang.spatial.operators.spark;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicateType;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.spatial.data.WGeometry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link SparkSpatialFilterOperator}.
 */
class SparkSpatialFilterOperatorTest {

    private Configuration configuration;
    private SparkExecutor sparkExecutor;
    private Job job;

    @BeforeEach
    void setUp() {
        WayangContext context = new WayangContext(new Configuration());
        this.job = context.createJob("spark-spatial-filter-test", new WayangPlan());
        this.configuration = this.job.getConfiguration();
        this.ensureCrossPlatformExecutor();
        this.sparkExecutor = (SparkExecutor) SparkPlatform.getInstance().getExecutorFactory().create(this.job);
    }

    private void ensureCrossPlatformExecutor() {
        try {
            Field field = Job.class.getDeclaredField("crossPlatformExecutor");
            field.setAccessible(true);
            if (field.get(this.job) == null) {
                CrossPlatformExecutor executor = new CrossPlatformExecutor(this.job, new FullInstrumentationStrategy());
                field.set(this.job, executor);
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to initialize CrossPlatformExecutor for tests.", e);
        }
    }

    private OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job);
        return optimizationContext.addOneTimeOperator(operator);
    }

    private void evaluate(SparkExecutionOperator operator,
                          ChannelInstance[] inputs,
                          ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, this.sparkExecutor, this.createOperatorContext(operator));
    }

    private RddChannel.Instance createRddChannelInstance() {
        return (RddChannel.Instance) RddChannel.UNCACHED_DESCRIPTOR
                .createChannel(null, this.configuration)
                .createInstance(mock(SparkExecutor.class), null, -1);
    }

    private RddChannel.Instance createRddChannelInstance(Collection<?> collection) {
        RddChannel.Instance instance = createRddChannelInstance();
        instance.accept(this.sparkExecutor.sc.parallelize(WayangCollections.asList(collection)), this.sparkExecutor);
        return instance;
    }

    @Test
    void testIntersectsFilter() {
        List<WGeometry> input = Arrays.asList(
                new WGeometry("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                new WGeometry("POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"),
                new WGeometry("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"),
                new WGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        WGeometry reference = new WGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

        SparkSpatialFilterOperator<WGeometry> filterOp = new SparkSpatialFilterOperator<>(
                SpatialPredicateType.INTERSECTS,
                w -> w,
                DataSetType.createDefault(WGeometry.class),
                reference
        );

        RddChannel.Instance inputChannel = this.createRddChannelInstance(input);
        RddChannel.Instance outputChannel = this.createRddChannelInstance();

        this.evaluate(filterOp,
                new ChannelInstance[]{inputChannel},
                new ChannelInstance[]{outputChannel});

        List<WGeometry> result = outputChannel.<WGeometry>provideRdd().collect();
        assertEquals(3, result.size());
    }

    @Test
    void testWithinFilter() {
        List<WGeometry> input = Arrays.asList(
                new WGeometry("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                new WGeometry("POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"),
                new WGeometry("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"),
                new WGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        WGeometry reference = new WGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

        SparkSpatialFilterOperator<WGeometry> filterOp = new SparkSpatialFilterOperator<>(
                SpatialPredicateType.WITHIN,
                w -> w,
                DataSetType.createDefault(WGeometry.class),
                reference
        );

        RddChannel.Instance inputChannel = this.createRddChannelInstance(input);
        RddChannel.Instance outputChannel = this.createRddChannelInstance();

        this.evaluate(filterOp,
                new ChannelInstance[]{inputChannel},
                new ChannelInstance[]{outputChannel});

        List<WGeometry> result = outputChannel.<WGeometry>provideRdd().collect();
        assertEquals(1, result.size());
    }

    @Test
    void testFilterNoMatches() {
        List<WGeometry> input = Arrays.asList(
                new WGeometry("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                new WGeometry("POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"),
                new WGeometry("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))"),
                new WGeometry("POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))")
        );

        WGeometry reference = new WGeometry("POLYGON ((100 100, 101 100, 101 101, 100 101, 100 100))");

        SparkSpatialFilterOperator<WGeometry> filterOp = new SparkSpatialFilterOperator<>(
                SpatialPredicateType.INTERSECTS,
                w -> w,
                DataSetType.createDefault(WGeometry.class),
                reference
        );

        RddChannel.Instance inputChannel = this.createRddChannelInstance(input);
        RddChannel.Instance outputChannel = this.createRddChannelInstance();

        this.evaluate(filterOp,
                new ChannelInstance[]{inputChannel},
                new ChannelInstance[]{outputChannel});

        List<WGeometry> result = outputChannel.<WGeometry>provideRdd().collect();
        assertEquals(0, result.size());
    }
}
