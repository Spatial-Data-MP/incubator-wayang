package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.WGeometry;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

/**
 * Logical operator representing a GeoJSON file source producing {@link WGeometry} elements.
 */
public class GeoJsonFileSource extends UnarySource<WGeometry> {

    private final String inputUrl;

    public GeoJsonFileSource(String inputUrl) {
        super(DataSetType.createDefault(WGeometry.class));
        this.inputUrl = inputUrl;
    }

    public GeoJsonFileSource(GeoJsonFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
    }

    public String getInputUrl() {
        return inputUrl;
    }
}
