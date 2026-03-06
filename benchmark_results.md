# Spatial Query Benchmark: Wayang vs Raw Postgres

## Results Summary

| Query | Wayang avg (ms) | Postgres avg (ms) | Overhead |
|-------|-----------------|-------------------|----------|
| spatial_filter | 445.71 | 35.17 | ~12.7x |
| spatial_join | 134.22 | 36.43 | ~3.7x |
| filtered_join | 124.41 | 36.11 | ~3.4x |

## Detailed Results

### Spatial Filter (Q1)
| Run | Wayang (ms) | Postgres (ms) |
|-----|-------------|---------------|
| 1 | 1760.94 | 37.06 |
| 2 | 145.62 | 35.60 |
| 3 | 111.02 | 34.23 |
| 4 | 119.28 | 34.03 |
| 5 | 91.72 | 34.93 |
| **avg** | **445.71** | **35.17** |

### Spatial Join (Q2)
| Run | Wayang (ms) | Postgres (ms) |
|-----|-------------|---------------|
| 1 | 153.35 | 36.74 |
| 2 | 136.12 | 37.13 |
| 3 | 131.82 | 35.84 |
| 4 | 136.69 | 35.46 |
| 5 | 113.11 | 37.00 |
| **avg** | **134.22** | **36.43** |

### Filtered Join (Q3)
| Run | Wayang (ms) | Postgres (ms) |
|-----|-------------|---------------|
| 1 | 132.84 | 36.48 |
| 2 | 158.78 | 36.06 |
| 3 | 116.85 | 37.37 |
| 4 | 111.79 | 36.69 |
| 5 | 101.79 | 33.94 |
| **avg** | **124.41** | **36.11** |

## Notes

- Wayang's `spatial_filter` average is skewed by the first run (1760ms cold start / JIT warmup). Excluding run 1, it averages ~117ms (~3.3x overhead), similar to the other queries.
- The Postgres times include psql process startup overhead (~30ms baseline per invocation).
- Each Wayang iteration creates a fresh `WayangContext` and `WayangPlan` to avoid caching effects.
- Database: PostgreSQL on localhost:5433/spiderdb with PostGIS extension.
- Tables: `spider_boxes` and `spider_boxes_2` (columns: id, x_min, y_min, x_max, y_max, geom).
