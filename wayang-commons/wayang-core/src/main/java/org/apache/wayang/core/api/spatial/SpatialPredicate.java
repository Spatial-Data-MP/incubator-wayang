package org.apache.wayang.core.api.spatial;

/**
 * Spatial relationship predicates for filtering and joining.
 * This enum provides a dependency-free way to specify spatial predicates
 * in the core module. The spatial plugin maps these to actual JTS predicates.
 */
public enum SpatialPredicate {

    /** Tests if geometries intersect (share any portion of space). */
    INTERSECTS,

    /** Tests if the first geometry contains the second. */
    CONTAINS,

    /** Tests if the first geometry is within the second. */
    WITHIN,

    /** Tests if geometries overlap (share some but not all interior points). */
    OVERLAPS,

    /** Tests if geometries touch (share a boundary but not interiors). */
    TOUCHES,

    /** Tests if geometries cross (share some interior points). */
    CROSSES,

    /** Tests if geometries are disjoint (share no points). */
    DISJOINT,

    /** Tests if geometries are topologically equal. */
    EQUALS
}
