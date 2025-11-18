//package org.apache.wayang.basic.data;
//
//import org.locationtech.jts.geom.Geometry;
//import org.locationtech.jts.io.ParseException;
//import org.locationtech.jts.io.WKBReader;
//import org.locationtech.jts.io.WKTReader;
//import org.postgresql.util.PGobject;
//
//import javax.xml.bind.DatatypeConverter;
//
///**
// * A specialization of {@link Record} for spatial data.
// */
//public class SpatialRecord extends Record {
//
//
//    public SpatialRecord(Object... values) {
//        super(values);
//    }
//
//    /**
//     * Retrieve a field as a {@link Geometry}. It must be castable as such.
//     *
//     * @param index the index of the field
//     * @return the {@link Geometry} representation of the field
//     */
////    public Geometry getGeometry(final int index, final WKBReader reader) throws ParseException {
////        final Object field = this.getValues()[index];
////        if (field instanceof Geometry) {
////            return (Geometry) field;
////        } if (field instanceof String) {
//////            return reader.read(DatatypeConverter.parseHexBinary(((PGobject) this.getField(index))).getValue()));
////            return reader.read(DatatypeConverter.parseHexBinary(((PGobject) this.getField(index)).getValue()));
////        } else {
////            throw new ClassCastException("Field at index " + index + " is not a Geometry: " + field);
////        }
////    }
//
//    public Geometry getGeometry(final int index, final WKBReader reader) throws ParseException {
//        final Object field = this.getValues()[index];
//
//        if (field instanceof Geometry) {
//            // Already a Geometry object
//            return (Geometry) field;
//        } else if (field instanceof PGobject) {
//            // Handle PostGIS geometry stored as a PGobject
//            final PGobject pgObj = (PGobject) field;
//            final String value = pgObj.getValue();
//            if (value == null) {
//                return null;
//            }
//            // Convert hex string to binary and parse as WKB
//            return reader.read(DatatypeConverter.parseHexBinary(value));
//        } else if (field instanceof String) {
//            // Handle raw hex string geometry
//            return reader.read(DatatypeConverter.parseHexBinary((String) field));
//        } else {
//            throw new ClassCastException("Field at index " + index + " is not a Geometry or PGobject: " + field);
//        }
//    }
//}
