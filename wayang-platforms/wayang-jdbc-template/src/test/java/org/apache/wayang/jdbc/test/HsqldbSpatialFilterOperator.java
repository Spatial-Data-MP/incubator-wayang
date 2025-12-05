package org.apache.wayang.jdbc.test;

import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.operators.JdbcSpatialJoinOperator;

/**
 * Test implementation of a spatial filter operator for HSQLDB.
 */
public class HsqldbSpatialFilterOperator extends JdbcSpatialJoinOperator {

    public HsqldbSpatialFilterOperator(
            TransformationDescriptor<Record, WGeometry> keyDescriptor0,
            TransformationDescriptor<Record, WGeometry> keyDescriptor1,
            SpatialPredicate predicate
    ) {
        super(keyDescriptor0, keyDescriptor1, predicate);
    }

    @Override
    public HsqldbPlatform getPlatform() {
        return HsqldbPlatform.getInstance();
    }
}
