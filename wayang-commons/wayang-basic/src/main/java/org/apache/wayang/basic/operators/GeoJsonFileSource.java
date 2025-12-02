package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

/**
 * Logical operator representing a GeoJSON file source producing {@link Record} elements.
 */
public class GeoJsonFileSource extends UnarySource<Record> {

    private final String inputUrl;

    public GeoJsonFileSource(String inputUrl) {
        super(DataSetType.createDefault(Record.class));
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
