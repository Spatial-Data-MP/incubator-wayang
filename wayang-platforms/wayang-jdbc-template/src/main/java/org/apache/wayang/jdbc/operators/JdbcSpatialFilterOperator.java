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

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.locationtech.jts.geom.Geometry;

import java.sql.Connection;


/**
 * Template for JDBC-based {@link FilterOperator}.
 */
public abstract class JdbcSpatialFilterOperator extends SpatialFilterOperator implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param filterType the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public JdbcSpatialFilterOperator(String filterType, Integer columnIndex, WGeometry geometry) {
        super(filterType, columnIndex, geometry, /*, DataSetType.createDefault(Record.class)*/"");
        if (this.geometryColumnIndex < 0) {
            throw new IllegalArgumentException("Column index must be >= 0.");
        }
    }

    public JdbcSpatialFilterOperator(SpatialFilterOperator that) {
        super(that);
    }


    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        if (this.referenceGeometry == null) {
            throw new IllegalStateException("Geometry for spatial filter must not be null.");
        }
        if (this.geometryColumnSqlName == null || this.geometryColumnSqlName.isEmpty()) {
            throw new IllegalStateException("geometryColumnSqlName must be set in SpatialFilterOperator.");
        }

        // Column expression (e.g. "geom" or "t.geom")
        final String columnExpr = this.geometryColumnSqlName;

        // Geometry literal as ST_GeomFromText('WKT', srid)
        final String wkt = this.referenceGeometry.getWKT();
        // TODO: Check which SRID to use.
        final int srid = 4326;

        final String geomLiteral;
        if (srid > 0) {
            geomLiteral = String.format("ST_GeomFromText('%s', %d)", wkt, srid);
        } else {
            geomLiteral = String.format("ST_GeomFromText('%s')", wkt);
        }

        // Use the SpatialRelation to generate the SQL instead of a switch.
        // If you allow null filterType → default to INTERSECTS, that should be
        // handled when initializing `relation` in SpatialFilterOperator.
        if (this.relation == null) {
            throw new IllegalStateException("Spatial relation must not be null.");
        }

        return this.relation.toSql(columnExpr, geomLiteral);
    }



    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.filter.load", this.getPlatform().getPlatformId());
    }

//    @Override
//    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
//        final Optional<LoadProfileEstimator> optEstimator =
//                JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
//        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.predicateDescriptor, configuration);
//        return optEstimator;
//    }
}
