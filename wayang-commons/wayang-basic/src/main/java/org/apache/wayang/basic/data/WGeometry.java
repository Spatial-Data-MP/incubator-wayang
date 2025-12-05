/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.basic.data;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.*;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class WGeometry implements Serializable {

    private final HashMap<String, Object> data;

    public WGeometry() {
        this.data = new HashMap<>();
    }

    /**
     * Backwards-compatible constructor, treats input as WKT.
     */
    public WGeometry(String wkt) {
        this();
        this.data.put("wkt", wkt);
    }
    /**
     * Create WGeometry from string input.
     * Detects WKT, WKB-hex, or GeoJSON and stores only that
     * representation initially. Other conversions are done lazily.
     *
     * @param input geometry string (WKT / WKB-hex / GeoJSON)
     * @return WGeometry instance
     */
    public static WGeometry fromStringInput(String input) {
        String trimmed = input.trim();
        WGeometry wg = new WGeometry();

        if (wg.looksLikeWKT(trimmed)) {
            wg.data.put("wkt", trimmed);
        } else if (wg.looksLikeGeoJSON(trimmed)) {
            wg.data.put("geojson", trimmed);
        } else {
            // Assume WKB hex string
            wg.data.put("wkb", trimmed);
        }

        return wg;
    }

    /**
     * Create WGeometry from an existing JTS Geometry object.
     * The geometry is stored, and all other formats (WKT/WKB/GeoJSON)
     * are generated lazily when their getters are called.
     *
     * @param geometry JTS Geometry instance
     * @return WGeometry wrapper
     */
    public static WGeometry fromGeometry(Geometry geometry) {
        if (geometry == null) {
            throw new IllegalArgumentException("Geometry must not be null.");
        }
        WGeometry wg = new WGeometry();
        wg.data.put("geometry", geometry);
        return wg;
    }

    public static WGeometry fromGeoJson(String geoJson) {
        WGeometry wg = new WGeometry();
        wg.data.put("geojson", geoJson);
        // could directely create the rescpective geometry with jts
        return wg;
    }

    /**
     * Get the geometry as WKT. If WKT is not yet available, it is
     * generated from another stored representation and cached.
     *
     * @return WKT string
     */
    public String getWKT() {
        Object wktObj = this.data.get("wkt");
        if (wktObj != null) {
            return wktObj.toString();
        }

        Geometry geometry = getGeometry();
        WKTWriter writer = new WKTWriter();
        String wkt = writer.write(geometry);
        this.data.put("wkt", wkt);
        return wkt;
    }

    /**
     * Get the geometry as WKB hex string. If WKB is not yet available,
     * it is generated from another stored representation and cached.
     *
     * @return WKB hex string
     */
    public String getWKB() {
        Object wkbObj = this.data.get("wkb");
        if (wkbObj != null) {
            return wkbObj.toString();
        }

        Geometry geometry = getGeometry();
        WKBWriter writer = new WKBWriter();
        byte[] wkbBytes = writer.write(geometry);
        String wkbHex = javax.xml.bind.DatatypeConverter.printHexBinary(wkbBytes);
        this.data.put("wkb", wkbHex);
        return wkbHex;
    }

    /**
     * Get the geometry as GeoJSON string. If GeoJSON is not yet
     * available, it is generated from another stored representation
     * and cached.
     *
     * @return GeoJSON string
     */
    public String getGeoJSON() {
        Object geoJsonObj = this.data.get("geojson");
        if (geoJsonObj != null) {
            return geoJsonObj.toString();
        }

        Geometry geometry = getGeometry();
        GeoJsonWriter writer = new GeoJsonWriter();
        String geoJson = writer.write(geometry);
        this.data.put("geojson", geoJson);
        return geoJson;
    }

    /**
     * Convert one of the stored geometry representations (WKT, WKB-hex,
     * or GeoJSON) into a JTS Geometry object.
     *
     * The first available representation is used in this order:
     * WKT -> WKB-hex -> GeoJSON
     *
     * The resulting Geometry is cached in the data map under "geometry".
     *
     * @return JTS Geometry instance
     */
    public Geometry getGeometry() {
        Object geomObj = this.data.get("geometry");
        if (geomObj instanceof Geometry) {
            return (Geometry) geomObj;
        }

        GeometryFactory gf = new GeometryFactory();
        Geometry geometry;

        try {
            if (this.data.containsKey("wkt")) {
                String wkt = cleanSRID(this.data.get("wkt").toString().trim());
                WKTReader reader = new WKTReader(gf);
                geometry = reader.read(wkt);

            } else if (this.data.containsKey("wkb")) {
                String wkbHex = this.data.get("wkb").toString().trim();
                byte[] wkbBytes = javax.xml.bind.DatatypeConverter.parseHexBinary(wkbHex);
                WKBReader reader = new WKBReader(gf);
                geometry = reader.read(wkbBytes);

            } else if (this.data.containsKey("geojson")) {
                String geoJson = this.data.get("geojson").toString().trim();
                GeoJsonReader reader = new GeoJsonReader(gf);
                geometry = reader.read(geoJson);

            } else {
                throw new IllegalStateException("No geometry representation available in WGeometry.");
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse geometry from stored representations.", e);
        }

        this.data.put("geometry", geometry);
        return geometry;
    }

    // ---------- Helpers ---------- //

    private boolean looksLikeWKT(String s) {
        return s.startsWith("SRID=") ||
                s.startsWith("POINT") ||
                s.startsWith("LINESTRING") ||
                s.startsWith("POLYGON") ||
                s.startsWith("MULTI") ||
                s.startsWith("GEOMETRYCOLLECTION");
    }

    private boolean looksLikeGeoJSON(String s) {
        return s.startsWith("{") && s.contains("\"type\"");
    }

    private String cleanSRID(String wkt) {
        if (wkt.startsWith("SRID=")) {
            int idx = wkt.indexOf(';');
            if (idx > 0 && idx < wkt.length() - 1) {
                return wkt.substring(idx + 1);
            }
        }
        return wkt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WGeometry)) return false;

        WGeometry that = (WGeometry) o;

        Geometry g1 = this.getGeometry();
        Geometry g2 = that.getGeometry();

        if (g1 == null || g2 == null) {
            return g1 == g2;
        }

        // Delegate to JTS Geometry equality (structural / topological, depending on JTS version).
        return g1.equals(g2);
    }

    @Override
    public int hashCode() {
        Geometry geometry = this.getGeometry();
        return geometry != null ? geometry.hashCode() : 0;
    }

}
