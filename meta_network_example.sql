-- ==================================================================================================
-- This SQL file demonstrates creating a meta-network from a given road network using Geohash cells.
-- The meta-network is a higher-level graph where each node represents a spatial cell (like a geohash
-- cell), and edges represent connectivity between these cells. The process allows:
-- 1. Creating a meta-network of Geohash cells covering the region.
-- 2. Building adjacency relations between these cells.
-- 3. Running a first-level (meta) routing on the Geohash-level network to find a coarse, high-level
--    path.
-- 4. Using that meta-network path to restrict the final detailed routing to only those portions of
--    the road network covered by the chosen Geohash cells, thus speeding up or simplifying the
--    routing process in large graphs.
--
-- Prerequisites:
-- - 'ways' table representing the road network edges with columns: gid, source, target, cost, the_geom
-- - 'ways_vertices_pgr' table representing the road network vertices with columns: id, the_geom
-- - pgRouting and PostGIS extensions enabled
--
-- NOTE: You will need to adapt coordinate bounds, start/end points, geohash precision, and tag filters
-- to match your dataset and scenario.
-- ==================================================================================================


-- Ensure required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;

-- Add geohash columns to ways and vertices tables for indexing by geohash
ALTER TABLE ways ADD COLUMN geohash TEXT;
ALTER TABLE ways_vertices_pgr ADD COLUMN geohash TEXT;

-- Update ways_vertices_pgr geohash values at specified precision (e.g., 5)
UPDATE ways_vertices_pgr 
SET geohash = ST_GeoHash(the_geom, 5);

-- Update ways geohash values by using centroid of each edge
UPDATE ways
SET geohash = ST_GeoHash(ST_Centroid(the_geom), 5);


-- ================================================================================================
-- Create a grid of Geohash cells covering the network area
-- ================================================================================================
-- The following function generate_geohash_grid creates a table of geohash cells at a given precision.

CREATE TABLE IF NOT EXISTS geohash_grid (
    geohash TEXT PRIMARY KEY,
    geom GEOMETRY(Polygon, 4326),
    grid_id SERIAL
);

-- This function generates geohash polygons covering a bounding box.
-- Adjust the bounding box and precision as needed.
CREATE OR REPLACE FUNCTION generate_geohash_grid(
    min_lon FLOAT,
    min_lat FLOAT,
    max_lon FLOAT,
    max_lat FLOAT,
    precisio INTEGER DEFAULT 5,
    srid INTEGER DEFAULT 4326
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    lon FLOAT := min_lon;
    lat FLOAT := min_lat;
    current_geohash TEXT;
    current_geom GEOMETRY;
    next_lon FLOAT;
    next_lat FLOAT;
BEGIN
    WHILE lat <= max_lat LOOP
        WHILE lon <= max_lon LOOP
            SELECT ST_GeoHash(ST_SetSRID(ST_MakePoint(lon, lat), srid), precisio) INTO current_geohash;
            SELECT ST_GeomFromGeoHash(current_geohash, precisio) INTO current_geom;
            SELECT ST_XMax(current_geom) INTO next_lon;
            SELECT ST_YMax(current_geom) INTO next_lat;

            INSERT INTO geohash_grid (geohash, geom)
            VALUES (current_geohash, current_geom)
            ON CONFLICT (geohash) DO NOTHING;

            lon := next_lon; 
        END LOOP;
        lon := min_lon;
        lat := next_lat;
    END LOOP;
END;
$$;

-- Determine bounding box from your vertices (adjust names as needed)
-- SELECT min(ST_X(the_geom)), max(ST_X(the_geom)), min(ST_Y(the_geom)), max(ST_Y(the_geom)) FROM ways_vertices_pgr;
-- Use these as input to generate the grid
-- Example:
-- SELECT generate_geohash_grid(3.3472141, 50.7339983, 7.2669184, 53.5414038, 5);


-- ================================================================================================
-- Building the Meta-Network (Adjacency between Geohash Cells)
-- ================================================================================================
-- Once we have a grid of geohash cells, we need to know which cells are adjacent.
-- Adjacent cells are those that share a border (not just a point) so we can build a graph of cells.

CREATE TABLE IF NOT EXISTS adjacent_geohashes (
    geohash TEXT,
    adjacent_geohash TEXT,
    PRIMARY KEY (geohash, adjacent_geohash)
);

-- Insert adjacency by checking intersection of polygons
INSERT INTO adjacent_geohashes (geohash, adjacent_geohash)
SELECT 
    a.geohash,
    b.geohash
FROM 
    geohash_grid a, 
    geohash_grid b
WHERE 
    a.geohash != b.geohash 
    AND ST_Intersects(a.geom, b.geom)
    AND NOT ST_Relate(a.geom, b.geom, 'FF*F0****');  -- Avoid corner-only contacts, must share an edge


-- ================================================================================================
-- Mapping Geohash Cells to Their Network Vertices
-- ================================================================================================
-- For routing at the geohash level, we need representative vertices from the underlying road network
-- within each geohash. We'll pick a representative vertex to connect to when routing between cells.

-- First, ensure that ways_vertices_pgr have geohash assigned (already done above).
-- Now find a representative vertex in each geohash cell.
-- We'll pick the closest vertex to the cell's polygon centroid or just the first vertex found.

ALTER TABLE adjacent_geohashes ADD COLUMN source_vertex_id INTEGER;
ALTER TABLE adjacent_geohashes ADD COLUMN dest_vertex_id INTEGER;

-- Update source_vertex_id
UPDATE adjacent_geohashes SET source_vertex_id = sub.id
FROM (
    SELECT 
        adjacent_geohashes.geohash,
        ways_vertices_pgr.id,
        ROW_NUMBER() OVER (
            PARTITION BY adjacent_geohashes.geohash 
            ORDER BY ST_Distance(ways_vertices_pgr.the_geom, (SELECT geom FROM geohash_grid WHERE geohash = adjacent_geohashes.geohash))
        ) AS rn
    FROM 
        adjacent_geohashes, ways_vertices_pgr
    WHERE adjacent_geohashes.geohash = ways_vertices_pgr.geohash
) AS sub
WHERE 
    adjacent_geohashes.geohash = sub.geohash 
    AND sub.rn = 1;

-- Update dest_vertex_id
UPDATE adjacent_geohashes SET dest_vertex_id = sub.id
FROM (
    SELECT 
        adjacent_geohashes.adjacent_geohash,
        ways_vertices_pgr.id,
        ROW_NUMBER() OVER (
            PARTITION BY adjacent_geohashes.adjacent_geohash 
            ORDER BY ST_Distance(ways_vertices_pgr.the_geom, (SELECT geom FROM geohash_grid WHERE geohash = adjacent_geohashes.adjacent_geohash))
        ) AS rn
    FROM 
        adjacent_geohashes, ways_vertices_pgr
    WHERE adjacent_geohashes.adjacent_geohash = ways_vertices_pgr.geohash
) AS sub
WHERE 
    adjacent_geohashes.adjacent_geohash = sub.adjacent_geohash 
    AND sub.rn = 1;


-- Remove rows where we couldn't find appropriate vertices
DELETE FROM adjacent_geohashes WHERE source_vertex_id IS NULL OR dest_vertex_id IS NULL;


-- ================================================================================================
-- Constructing the Meta-Network Edges with Aggregated Costs
-- ================================================================================================
-- We'll create a table to store the aggregated cost between geohash pairs.
-- The idea: 
-- - Use pgr_dijkstraCost or pgr_dijkstra to find minimal cost between representative vertices of
--   adjacent geohash cells.
-- - Store this aggregated cost in a meta-network table.

CREATE TABLE IF NOT EXISTS geohash_subgraph_car (
    source_geohash TEXT,
    dest_geohash TEXT,
    aggregated_cost FLOAT,
    PRIMARY KEY (source_geohash, dest_geohash)
);

-- Add references to the geohash_grid IDs for routing in the meta-network
ALTER TABLE geohash_subgraph_car ADD COLUMN src_grid_id INT;
ALTER TABLE geohash_subgraph_car ADD COLUMN dst_grid_id INT;

UPDATE geohash_subgraph_car
SET src_grid_id = g.grid_id
FROM geohash_grid g
WHERE g.geohash = geohash_subgraph_car.source_geohash;

UPDATE geohash_subgraph_car
SET dst_grid_id = g.grid_id
FROM geohash_grid g
WHERE g.geohash = geohash_subgraph_car.dest_geohash;


-- ================================================================================================
-- Function to Calculate Fastest Routes Between Adjacent Cells
-- ================================================================================================
-- We'll use pgr_dijkstracost to compute the cost between all adjacent geohashes at once, then store.

CREATE OR REPLACE FUNCTION calculate_fastest_routes_for_adjacent_cells()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    cost_record RECORD;
    src_geohash TEXT;
    dst_geohash TEXT;
BEGIN
    FOR cost_record IN (
        SELECT * FROM pgr_dijkstracost(
            'SELECT gid as id, source, target, cost FROM ways',  -- Road network
            'SELECT source_vertex_id as source, dest_vertex_id as target FROM adjacent_geohashes', -- Pairs
            directed := false
        )
    ) LOOP
        -- Map vertex IDs back to their Geohashes
        SELECT geohash INTO src_geohash 
        FROM adjacent_geohashes 
        WHERE source_vertex_id = cost_record.start_vid LIMIT 1;

        SELECT adjacent_geohash INTO dst_geohash 
        FROM adjacent_geohashes 
        WHERE dest_vertex_id = cost_record.end_vid LIMIT 1;

        IF src_geohash IS NOT NULL AND dst_geohash IS NOT NULL THEN
            INSERT INTO geohash_subgraph_car (source_geohash, dest_geohash, aggregated_cost)
            VALUES (src_geohash, dst_geohash, cost_record.agg_cost)
            ON CONFLICT (source_geohash, dest_geohash) 
            DO UPDATE SET aggregated_cost = EXCLUDED.aggregated_cost;
        END IF;
    END LOOP;
END;
$$;

-- Run the calculation
SELECT calculate_fastest_routes_for_adjacent_cells();


-- Clean up any meta-network edges not consistent with adjacency
DELETE FROM geohash_subgraph_car
WHERE NOT EXISTS (
    SELECT 1
    FROM adjacent_geohashes
    WHERE 
        geohash_subgraph_car.source_geohash = adjacent_geohashes.geohash AND
        geohash_subgraph_car.dest_geohash = adjacent_geohashes.adjacent_geohash
);


-- ================================================================================================
-- Meta-Network Routing
-- ================================================================================================
-- Now we have a meta-network in geohash_subgraph_car connecting grid cells. 
-- We can run a coarse path search at the geohash level. Suppose we have start and end geohash cells.

-- Example: Find meta-network path from a start cell (mapped to src_grid_id=5023) to an end cell (dst_grid_id=3367).
-- Adjust these IDs to match your start/end geohash cells.

-- pgr_dijkstra on meta-level:
-- Need a proper network definition:
-- geohash_subgraph_car now forms a graph with src_grid_id and dst_grid_id as source/target.

-- Ensure src_grid_id and dst_grid_id are not NULL
-- If needed, run:
-- UPDATE geohash_subgraph_car
-- SET src_grid_id = g.grid_id FROM geohash_grid g WHERE geohash_subgraph_car.source_geohash = g.geohash;
-- UPDATE geohash_subgraph_car
-- SET dst_grid_id = g.grid_id FROM geohash_grid g WHERE geohash_subgraph_car.dest_geohash = g.geohash;


-- Running meta routing:
-- This returns a path of grid_ids:
CREATE TEMP TABLE subgraph AS (
    SELECT * 
    FROM pgr_dijkstra(
        'SELECT id as id, src_grid_id as source, dst_grid_id as target, aggregated_cost as cost FROM geohash_subgraph_car',
        5023,  -- Example start grid_id (corresponding to start geohash)
        3367,  -- Example end grid_id (corresponding to end geohash)
        directed := false
    )
);


-- Extract the geohashes along the optimal meta path:
-- This gives us a set of geohash cells that the path covers.
-- We'll use these cells to filter the final route computation at the road level.

SELECT STRING_AGG(a.geohash::text, ',') AS path_geohashes
FROM subgraph s
INNER JOIN geohash_grid a ON a.grid_id = s.node;


-- ================================================================================================
-- Final Detailed Routing
-- ================================================================================================
-- With the meta-level path obtained (list of geohash cells), run pgr_dijkstra again on the actual 'ways'
-- table, but this time restricted to edges that have their geohash in the final path cells.

-- Example final routing:
-- Replace 67926 and 310770 with actual vertex IDs for start/end points at the road level.

SELECT * 
FROM pgr_dijkstra(
    'SELECT gid as id, source, target, cost FROM ways WHERE geohash IN (
        SELECT a.geohash FROM subgraph s 
        INNER JOIN geohash_grid a ON a.grid_id = s.node
    )',
    67926,
    310770,
    directed := false
);


-- ================================================================================================
-- Optional: Create a function that automatically performs the final path finding
-- by dynamically building the WHERE clause for the underlying network based on the
-- meta-network path results:
-- (Adjust the IDs and logic as needed.)

DROP FUNCTION IF EXISTS find_optimal_path();
CREATE OR REPLACE FUNCTION find_optimal_path() 
RETURNS TABLE(seq int, edge int, cost float, agg_cost float) AS $$
DECLARE 
    geohash_cond TEXT := '';
    geohashes TEXT[];
BEGIN
    WITH OptimalGeohashPath AS (
        SELECT node 
        FROM pgr_dijkstra(
            'SELECT id as id, src_grid_id as source, dst_grid_id as target, aggregated_cost as cost FROM geohash_subgraph_car',
            5023,  -- start grid id
            3367,  -- end grid id
            directed := false
        )
    ), nodes AS (
        SELECT a.geohash
        FROM OptimalGeohashPath o
        INNER JOIN geohash_grid a ON a.grid_id = o.node
    )
    SELECT ARRAY_AGG(a.geohash) INTO geohashes FROM nodes a;

    geohash_cond := (SELECT STRING_AGG('geohash = ''' || g || '''', ' OR ') FROM unnest(geohashes) g);

    RETURN QUERY EXECUTE 'SELECT * FROM pgr_dijkstra(
        ''SELECT gid as id, source, target, cost FROM ways WHERE ' || geohash_cond || ''',
        67926,
        310770,
        false
    )';
END;
$$ LANGUAGE plpgsql;

-- Usage:
-- SELECT * FROM find_optimal_path();


-- ================================================================================================
-- Additional Adjustments
-- ================================================================================================
-- Example of adjusting costs or reverse_cost for certain tags:
-- (Adjust tags and factors as needed)
UPDATE ways 
SET reverse_cost = cost * 0.01
WHERE tag_id IN (103, 102, 101);


-- Make sure to create indexes for performance if needed:
-- CREATE INDEX idx_ways_geohash ON ways(geohash);
-- CREATE INDEX idx_ways_vertices_pgr_geom ON ways_vertices_pgr USING gist(the_geom);
-- CREATE INDEX idx_geohash_grid_geom ON geohash_grid USING gist(geom);
-- CREATE INDEX idx_geohash_subgraph_car_source_target ON geohash_subgraph_car(src_grid_id, dst_grid_id);


-- ================================================================================================
-- END OF FILE
-- ================================================================================================
