package org.apache.wayang.java.mapping;

import org.apache.wayang.core.mapping.*;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

public class GeoJsonFileSource  implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new org.apache.wayang.basic.operators.GeoJsonFileSource((String) null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<org.apache.wayang.basic.operators.GeoJsonFileSource>(
                (matchedOperator, epoch) -> new org.apache.wayang.java.operators.JavaGeoJsonFileSource(matchedOperator).at(epoch)
        );
    }
}
