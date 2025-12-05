package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.operators.JdbcSpatialJoinOperator;

public class PostgresSpatialJoinOperator extends JdbcSpatialJoinOperator implements PostgresExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param predicate the type of spatial join (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public PostgresSpatialJoinOperator(TransformationDescriptor<Record, WGeometry> keyDescriptor0,
                                       TransformationDescriptor<Record, WGeometry> keyDescriptor1,
                                       SpatialPredicate predicate) {
        super(keyDescriptor0, keyDescriptor1, predicate);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresSpatialJoinOperator(SpatialJoinOperator that) {
        super(that);
    }

    @Override
    protected PostgresSpatialJoinOperator createCopy() {
        return new PostgresSpatialJoinOperator(this);
    }
}
