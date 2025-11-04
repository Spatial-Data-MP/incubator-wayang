package org.apache.wayang.basic.data;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * A specialization of {@link Record} for spatial data.
 */
public class SpatialRecord extends Record {
    /**
     * Retrieve a field as a {@link Geometry}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@link Geometry} representation of the field
     */
    public Geometry getGeometry(final int index, final WKTReader reader) throws ParseException {
        final Object field = this.getValues()[index];
        if (field instanceof Geometry) {
            return (Geometry) field;
        } if (field instanceof String) {
            return reader.read((String) field);
        } else {
            throw new ClassCastException("Field at index " + index + " is not a Geometry: " + field);
        }
    }
}
