package org.apache.wayang.basic.data.geometry;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.HashMap;

public class WGeometry implements Serializable
{
    HashMap<String, Object> data;


    public WGeometry()
    {
        this.data = new HashMap<>();
    }

    public WGeometry(String wkb)
    {
        this.data = new HashMap<>();
        this.data.put("wkt", wkb);
    }

    public String getWKT()
    {
        return this.data.get("wkt").toString();
    }

    public Geometry getGeometry()
    {
        // Return default geometry to start
        return new GeometryFactory().createPolygon(new org.locationtech.jts.geom.Coordinate[]{
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(1, 0),
                new org.locationtech.jts.geom.Coordinate(1, 1),
                new org.locationtech.jts.geom.Coordinate(0, 1),
                new org.locationtech.jts.geom.Coordinate(0, 0)
        });

//        return (Geometry) this.data.get("geometry");
    }

}
