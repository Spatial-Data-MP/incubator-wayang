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

package org.apache.wayang.jdbc.channels;

import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

import java.util.Objects;

/**
 * A {@link SqlQueryChannel} subclass for count queries. Uses its own {@link Descriptor}
 * so that the optimizer treats it as a distinct channel type, preventing the count-specific
 * conversion from being selected for regular queries.
 */
public class SqlCountQueryChannel extends SqlQueryChannel {

    public SqlCountQueryChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private SqlCountQueryChannel(SqlCountQueryChannel parent) {
        super(parent);
    }

    @Override
    public SqlCountQueryChannel copy() {
        return new SqlCountQueryChannel(this);
    }

    /**
     * {@link ChannelDescriptor} for {@link SqlCountQueryChannel}s.
     * Because {@link ChannelDescriptor#equals} uses {@code getClass()}, this is automatically
     * distinct from {@link SqlQueryChannel.Descriptor}.
     */
    public static class Descriptor extends ChannelDescriptor {

        private final JdbcPlatformTemplate platform;

        public Descriptor(JdbcPlatformTemplate platform) {
            super(SqlCountQueryChannel.class, false, false);
            this.platform = platform;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(platform, that.platform);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), platform);
        }
    }
}
