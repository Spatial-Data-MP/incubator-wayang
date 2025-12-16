package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

public abstract class JdbcSpatialJoinOperator
        extends SpatialJoinOperator<Record, Record>
        implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see SpatialJoinOperator#SpatialJoinOperator(Record, Record,...)
     */
    public JdbcSpatialJoinOperator(
            TransformationDescriptor<Record, WGeometry> keyDescriptor0,
            TransformationDescriptor<Record, WGeometry> keyDescriptor1,
            SpatialPredicate predicate
    ) {
        super(
                keyDescriptor0,
                keyDescriptor1,
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class),
                predicate
        );
    }

    /**
     * Copies an instance.
     *
     * @param that that should be copied
     */
    public JdbcSpatialJoinOperator(SpatialJoinOperator<Record, Record> that) {
        super(that);
    }

    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        final Tuple<String, String> left = this.keyDescriptor0.getSqlImplementation();
        final Tuple<String, String> right = this.keyDescriptor1.getSqlImplementation();
        final String leftTableName = left.field0;
        final String leftKey = left.field1;
        final String rightTableName = right.field0;
        final String rightKey = right.field1;

        return "JOIN " + rightTableName + " ON " +
                this.predicate.toSql(leftTableName, leftKey, rightTableName, rightKey);
    }
}
