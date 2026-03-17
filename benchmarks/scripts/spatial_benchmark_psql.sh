#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Spatial benchmark: runs 3 SQL queries via psql, 5 iterations each.
# Appends results to benchmark_results.csv.

set -euo pipefail

CSV_FILE="benchmark_results.csv"
NUM_RUNS=10
PSQL="psql -h localhost -p 5433 -U postgres -d spiderdb -t -A"
export PGPASSWORD="postgres"

# SQL queries
Q1="SELECT COUNT(*) FROM spider_boxes WHERE ST_Intersects(geom, ST_GeomFromText('POLYGON((0.30 0.30,0.70 0.30,0.70 0.70,0.30 0.70,0.30 0.30))', 4326))"

Q2="SELECT COUNT(*) FROM spider_boxes a, spider_boxes_2 b WHERE ST_Intersects(a.geom, b.geom)"

Q3="SELECT COUNT(*) FROM spider_boxes a, spider_boxes_2 b WHERE ST_Intersects(a.geom, ST_GeomFromText('POLYGON((0.30 0.30,0.70 0.30,0.70 0.70,0.30 0.70,0.30 0.30))', 4326)) AND ST_Intersects(b.geom, ST_GeomFromText('POLYGON((0.40 0.40,0.60 0.40,0.60 0.60,0.40 0.60,0.40 0.40))', 4326)) AND ST_Intersects(a.geom, b.geom)"

run_query() {
    local query_name="$1"
    local sql="$2"
    local total_ms=0

    for i in $(seq 1 "$NUM_RUNS"); do
        start_ns=$(date +%s%N)
        $PSQL -c "$sql" > /dev/null
        end_ns=$(date +%s%N)
        elapsed_ns=$((end_ns - start_ns))
        ms=$(awk "BEGIN {printf \"%.2f\", $elapsed_ns / 1000000.0}")
        total_ms=$(awk "BEGIN {printf \"%.2f\", $total_ms + $ms}")
        echo "${query_name},postgres,${i},${ms}" >> "$CSV_FILE"
        echo "[postgres] ${query_name} run ${i}: ${ms} ms"
    done

    avg=$(awk "BEGIN {printf \"%.2f\", $total_ms / $NUM_RUNS}")
    echo "${query_name},postgres,avg,${avg}" >> "$CSV_FILE"
    echo "[postgres] ${query_name} avg: ${avg} ms"
}

# Ensure CSV header exists if file is empty or missing
if [ ! -s "$CSV_FILE" ]; then
    echo "query_name,engine,run,runtime_ms" > "$CSV_FILE"
fi

Q4="SELECT a.*, b.* FROM spider_boxes a, spider_boxes_2 b WHERE ST_Intersects(a.geom, ST_GeomFromText('POLYGON((0.30 0.30,0.70 0.30,0.70 0.70,0.30 0.70,0.30 0.30))', 4326)) AND ST_Intersects(b.geom, ST_GeomFromText('POLYGON((0.40 0.40,0.60 0.40,0.60 0.60,0.40 0.60,0.40 0.40))', 4326)) AND ST_Intersects(a.geom, b.geom)"

Q5="SELECT a.*, b.* FROM spider_boxes_10k a, spider_boxes_10k_2 b WHERE ST_Intersects(a.geom, b.geom)"

Q6="SELECT a.*, b.* FROM spider_boxes_10k a, spider_boxes_10k_2 b WHERE ST_Intersects(a.geom, ST_GeomFromText('POLYGON((0.30 0.30,0.70 0.30,0.70 0.70,0.30 0.70,0.30 0.30))', 4326)) AND ST_Intersects(b.geom, ST_GeomFromText('POLYGON((0.40 0.40,0.60 0.40,0.60 0.60,0.40 0.60,0.40 0.40))', 4326)) AND ST_Intersects(a.geom, b.geom)"

run_query "spatial_filter" "$Q1"
run_query "spatial_join" "$Q2"
run_query "filtered_join" "$Q3"
run_query "postgres_filtered_join" "$Q4"
run_query "postgres_join_10k" "$Q5"
run_query "postgres_filtered_join_10k" "$Q6"

echo "Postgres benchmark complete. Results appended to $CSV_FILE"
