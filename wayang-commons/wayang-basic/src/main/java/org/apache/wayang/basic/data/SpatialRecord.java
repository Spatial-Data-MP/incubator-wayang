package org.apache.wayang.basic.data;

import org.apache.wayang.basic.data.geometry.Geometry;
import org.apache.wayang.basic.data.geometry.GeometryParser;
import org.postgresql.util.PGobject;

/**
 * A specialization of {@link Record} for spatial data.
 */
public class SpatialRecord extends Record {


    public SpatialRecord(Object... values) {
        super(values);
    }

    /**
     * Retrieve a field as a {@link Geometry}. The value must already be a Geometry instance.
     *
     * @param index the index of the field
     * @return the {@link Geometry} representation of the field
     */
    public Geometry getGeometry(final int index) {
        final Object field = this.getValues()[index];
        if (field == null) {
            return null;
        }
        if (field instanceof Geometry) {
            return (Geometry) field;
        }
        Geometry parsed = null;
        if (field instanceof PGobject) {
            parsed = GeometryParser.parse(((PGobject) field).getValue());
        } else if (field instanceof CharSequence) {
            parsed = GeometryParser.parse(field.toString());
        } else if (field instanceof byte[]) {
            parsed = GeometryParser.parse((byte[]) field);
        }
        if (parsed != null) {
            this.setField(index, parsed);
            return parsed;
        }
        if (field instanceof PGobject || field instanceof CharSequence || field instanceof byte[]) {
            return null;
        }
        throw new ClassCastException("Field at index " + index + " is not convertible to Geometry: " + field);
    }
}
