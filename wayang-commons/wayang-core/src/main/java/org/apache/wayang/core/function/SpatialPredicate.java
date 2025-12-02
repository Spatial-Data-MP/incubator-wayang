package org.apache.wayang.core.function;

import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.function.BiPredicate;

public enum SpatialPredicate {

    INTERSECTS("INTERSECTS", "ST_Intersects", Geometry::intersects),
    CONTAINS("CONTAINS", "ST_Contains", Geometry::contains),
    WITHIN("WITHIN", "ST_Within", Geometry::within),
    TOUCHES("TOUCHES", "ST_Touches", Geometry::touches),
    OVERLAPS("OVERLAPS", "ST_Overlaps", Geometry::overlaps),
    CROSSES("CROSSES", "ST_Crosses", Geometry::crosses),
    DISJOINT("DISJOINT", "ST_Disjoint", Geometry::disjoint),
    EQUALS("EQUALS", "ST_Equals", Geometry::equalsTopo);

    private final String opName;
    private final String sqlFunctionName;
    private final BiPredicate<Geometry, Geometry> predicate;

    SpatialPredicate(String opName,
                     String sqlFunctionName,
                     BiPredicate<Geometry, Geometry> predicate) {
        this.opName = opName;
        this.sqlFunctionName = sqlFunctionName;
        this.predicate = predicate;
    }

    public static SpatialPredicate fromString(String opName) {
        return Arrays.stream(values())
                .filter(r -> r.opName.equalsIgnoreCase(opName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported spatial filter type: " + opName));
    }

    public boolean test(Geometry candidate, Geometry reference) {
        return predicate.test(candidate, reference);
    }

    public String toSql(String columnExpr, String geomLiteral) {
        return String.format("%s(%s, %s)", this.sqlFunctionName, columnExpr, geomLiteral);
    }
}
