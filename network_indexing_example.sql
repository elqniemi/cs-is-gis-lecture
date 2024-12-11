-- ==================================================================================================
-- This SQL file demonstrates how to:
-- 1. Set up a spatial and routing-enabled database (PostGIS, pgRouting, H3).
-- 2. Load a real-life road network into a table.
-- 3. Add H3 indexing to the network for spatial indexing and cell-based selection.
-- 4. Use H3 cells to select parts of the network that fall within certain H3 indexes.
-- 5. Perform routing on the network using pgRouting and find nearest nodes for start/end points.
-- 6. Calculate statistics per H3 cell and run analyses on routes.
--
-- The logical order is as follows:
-- - Load extensions and create schemas.
-- - Create and index network tables.
-- - Demonstrate H3 indexing and functions that compute statistics on H3 cells.
-- - Show how to select network parts by H3 index.
-- - Set up a sample scooters trips table and demonstrate how to find nearest graph vertices.
-- - Perform routing and store results.
-- - Analyze routes intersecting with H3 cells.
-- ==================================================================================================


-- ================================================================================================
-- 1. Load Required Extensions
-- ================================================================================================
-- Enable PostGIS for spatial functions and geometry types
create extension if not exists postgis;

-- Enable pgRouting for graph and routing functionalities
create extension if not exists pgrouting;

-- Enable H3 extension for hexagonal hierarchical spatial indexing
create extension if not exists h3;


-- ================================================================================================
-- 2. Create Schemas
-- ================================================================================================
-- We'll separate data logically into schemas:
-- 'network' for road network data
-- 'scooters' for scooters trips data
-- 'analysis' for analysis and results
-- 'results' for storing computed results from analyses (like H3 cell results)
create schema if not exists scooters;
create schema if not exists network;
create schema if not exists analysis;
create schema if not exists results;


-- ================================================================================================
-- 3. Set Up the Network Tables
-- ================================================================================================
-- We assume 'network.ways' already contains the road network in some SRID (e.g., EPSG:28992),
-- which typically is loaded from OSM or other sources. If not, this snippet should be adapted.
-- The code below demonstrates transforming it to WGS84 and indexing.

-- Transform the ways to WGS84 for H3 indexing (H3 expects coordinates in lat/lng WGS84)
-- and create a table storing ways in EPSG:4326
create table network.ways_wgs as (
    select
        gid,
        tag_id,
        st_length(the_geom) as length,
        st_transform(the_geom, 4326) as the_geom
    from
        network.ways
);

-- Create a spatial index on the ways_wgs table to speed up spatial queries
create index idx_network_ways_wgs_geom on network.ways_wgs using gist(the_geom);


-- We also have vertices table (created by pgr_createTopology)
-- This table stores the nodes (intersections) of the network.
-- We'll transform its geometry to WGS84 as well.
-- In practice, you would run pgr_createTopology before this step.
select pgr_createtopology('network.ways', 0.001, clean:=true);

create table network.ways_wgs_vertices as 
select
    cnt,
    st_transform(the_geom, 4326)::point the_geom,
    st_transform(the_geom, 4326) as geom
from
    network.ways_vertices_pgr;

-- Create a spatial index for vertices
create index idx_network_ways_wgs_vertices_geom on network.ways_wgs_vertices using gist(geom);


-- ================================================================================================
-- 4. Working with H3 - Indexing the Network
-- ================================================================================================
-- We will demonstrate how to create H3 cells covering the network, store them,
-- and use them to select and analyze the network data.
-- H3 indexing is done by converting lat/lng points to H3 cells at a given resolution
-- and then retrieving the polygon boundary of those cells.

-- Example function: calculate_way_statistics (H3-based)
-- This function:
-- - Generates H3 cells at a given H3 resolution covering the network vertices.
-- - Calculates statistics such as bearing and density of ways intersecting each H3 cell.
-- - Returns a set of records for each H3 cell.
create or replace function calculate_way_statistics(h3_level int)
returns table(index text, bearing double precision, density double precision, h3_geom geometry) as $$
declare
    cell record;
begin
    -- Create a temporary table for H3 cells covering the network vertices
    create temp table if not exists temp_h3_cells as
    select distinct 
        h3_lat_lng_to_cell(the_geom, h3_level) as h3_cell,
        st_setsrid(h3_cell_to_boundary(h3_lat_lng_to_cell(the_geom, h3_level))::geometry, 4326) as cell_geom
    from 
        network.ways_wgs_vertices;

    for cell in
        select h3_cell, cell_geom from temp_h3_cells
    loop
        index := cell.h3_cell;
        h3_geom := cell.cell_geom;
        
        -- Calculate average bearing of ways inside the cell
        bearing := (
            select coalesce(avg(st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom))), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        
        -- Calculate density of ways inside the cell (ways count/area)
        density := (
            select coalesce(count(*), 0) / nullif(st_area(cell.cell_geom::geography), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        
        return next;
    end loop;

    drop table temp_h3_cells;
end;
$$ language plpgsql;


-- A more complex example function that calculates more statistics per H3 cell:
-- This version computes bearing, density, connectivity and various road type densities.
-- It's just an illustration of the richness of analyses we can perform.
create or replace function calculate_way_statistics_new(h3_level int)
returns table(
    index text, 
    bearing double precision, 
    density double precision, 
    connectivity double precision, 
    bicycle_lane_density double precision, 
    primary_density double precision, 
    secondary_density double precision, 
    other_density double precision, 
    ped_density double precision, 
    dead_end_density double precision, 
    h3_geom geometry
) as $$
declare
    cell record;
    rec record;
    total_length double precision;
    weighted_bearing_sum_x double precision;
    weighted_bearing_sum_y double precision;
    bicycle_lane_length double precision;
    primary_length double precision;
    secondary_length double precision;
    other_length double precision;
    ped_length double precision;
    dead_end_count int;
begin
    -- Create temp H3 cells over the network extent
    create temp table if not exists temp_h3_cells as
    with road_extent as (
        select st_envelope(st_collect(the_geom))::polygon as geom
        from network.ways_wgs
    )
    select distinct 
        h3_cell,
        st_setsrid(h3_cell_to_boundary(h3_cell)::geometry, 4326) as cell_geom
    from road_extent, 
         lateral h3_polygon_to_cells(geom, NULL, h3_level) h3_cell;

    for cell in select h3_cell, cell_geom from temp_h3_cells
    loop
        index := cell.h3_cell;
        h3_geom := cell.cell_geom;

        -- Weighted Bearing Calculation
        total_length := 0;
        weighted_bearing_sum_x := 0.0;
        weighted_bearing_sum_y := 0.0;

        for rec in
            select length, st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom)) as segment_bearing
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        loop
            if rec.segment_bearing is not null then
                total_length := total_length + rec.length;
                weighted_bearing_sum_x := weighted_bearing_sum_x + (rec.length * cos(rec.segment_bearing));
                weighted_bearing_sum_y := weighted_bearing_sum_y + (rec.length * sin(rec.segment_bearing));
            end if;
        end loop;

        bearing := case 
            when total_length = 0 then null 
            else atan2(weighted_bearing_sum_y, weighted_bearing_sum_x) 
        end;

        density := (
            select coalesce(sum(length), 0) / nullif(st_area(cell.cell_geom::geography), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );

        connectivity := (
            select coalesce(sum(length) / nullif(count(*), 0), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );

        -- Here we are filtering by some tags (tag_id) to identify different road types.
        -- Adjust these tag_id conditions according to your data schema.
        bicycle_lane_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (118, 201, 202, 203, 204)
        );

        primary_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id > 100 and tag_id < 108
        );

        secondary_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (108, 124, 109, 125)
        );

        other_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (110, 111, 112, 113, 100, 123)
        );

        ped_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (114, 117, 119, 122)
        );

        dead_end_count := (
            select count(*)
            from network.ways_wgs_vertices
            where cnt = 1
            and st_intersects(geom, cell.cell_geom)
        );

        bicycle_lane_density := bicycle_lane_length / nullif(st_area(cell.cell_geom::geography), 0);
        primary_density := primary_length / nullif(st_area(cell.cell_geom::geography), 0);
        secondary_density := secondary_length / nullif(st_area(cell.cell_geom::geography), 0);
        other_density := other_length / nullif(st_area(cell.cell_geom::geography), 0);
        ped_density := ped_length / nullif(st_area(cell.cell_geom::geography), 0);
        dead_end_density := dead_end_count / nullif(st_area(cell.cell_geom::geography), 0);

        return next;
    end loop;

    drop table temp_h3_cells;
end;
$$ language plpgsql;


-- Example result table storing the computed stats at H3 resolution 9
create table results.cell_9 as (
select 
    index,
    bearing,
    bearing * 180 / pi() as angle,
    density,
    connectivity,
    bicycle_lane_density,
    primary_density,
    secondary_density,
    other_density,
    ped_density,
    dead_end_density,
    h3_geom as geom
from calculate_way_statistics_new(9)
);


-- ================================================================================================
-- 5. Selecting Parts of the Network Using H3
-- ================================================================================================
-- Once we have cells (e.g., results.cell_9), we can join them to the network data
-- or select only those ways that fall into a given H3 cell.
-- Example: 
-- select * from network.ways_wgs w, results.cell_9 c
-- where c.index = '847b59dffffffff' -- (example H3 cell)
-- and st_intersects(w.the_geom, c.geom);


-- ================================================================================================
-- 6. Routing on the Network
-- ================================================================================================
-- pgRouting functions can compute shortest paths. 
-- Before routing, we ensure our network table is topology-ready.

-- Already done: pgr_createtopology('network.ways', 0.001, clean:=true);
-- create indexes on ways, vertices if needed
-- They are already done above.

-- Example: Using pgr_dijkstra, we can route between source and target nodes.
-- The code below shows how to find nearest nodes for start and end points of trips,
-- and then run a routing function to get the path.


-- Create an example scooters table to store vehicle locations and convert to geometry
-- This is sample data structure:
create table scooters.scooters_temp (
    car_id integer,
    location_id integer,
    vehicletype_id integer,
    isdamaged boolean,
    min_fuellevel numeric,
    max_fuellevel numeric,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    pricingtime text,
    geom_rd text
);

create table scooters.scooters as 
select
    car_id,
    location_id,
    vehicletype_id,
    isdamaged,
    min_fuellevel,
    max_fuellevel,
    start_time,
    end_time,
    substring(pricingtime, 4, 2)::int as price_cent,
    st_setsrid(st_geomfromtext(geom_rd), 28992)::geometry as geom
from
    scooters.scooters_temp;

-- Just checking data
select * from scooters.scooters;


-- A function to calculate distance just as an example (PostGIS can do st_distance directly)
create or replace function calculate_distance(geom1 geometry, geom2 geometry) returns double precision as $$
begin
    return st_distance(geom1, geom2);
end;
$$ language plpgsql;


-- Create a 'trips' table representing journeys of scooters from a start to an end point.
-- This uses window functions and a heuristic (distance > 100m) to mark a new trip.
-- It's just an example of building an OD (Origin-Destination) dataset from point logs.
create table scooters.trips (
    trip_id serial primary key,
    car_id integer,
    vehicletype_id integer,
    isdamaged boolean,
    avg_min_fuellevel numeric,
    avg_max_fuellevel numeric,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    price_cent integer,
    geom_start geometry,
    geom_end geometry
);

insert into scooters.trips (
    car_id,
    vehicletype_id,
    isdamaged,
    avg_min_fuellevel,
    avg_max_fuellevel,
    start_time,
    end_time,
    price_cent,
    geom_start,
    geom_end
)
with ordered_data as (
    select *,
           lead(geom, 1) over (partition by car_id order by end_time) as next_geom,
           lead(start_time, 1) over (partition by car_id order by end_time) as next_start_time
    from scooters.scooters
),
trip_markers as (
    select *,
           case when calculate_distance(geom, coalesce(next_geom, geom)) > 100 
                then 1 else 0 end as is_trip
    from ordered_data
),
trip_boundaries as (
    select *,
           sum(is_trip) over (partition by car_id order by end_time) as trip_id_group
    from trip_markers
)
select 
    min(car_id) as car_id,
    min(vehicletype_id) as vehicletype_id,
    bool_or(isdamaged) as isdamaged,
    avg(min_fuellevel) as avg_min_fuellevel,
    avg(max_fuellevel) as avg_max_fuellevel,
    max(end_time) as start_time,
    min(next_start_time) as end_time,
    sum(price_cent) as price_cent,
    min(geom) as geom_start,
    max(next_geom) as geom_end
from trip_boundaries
where is_trip = 1
group by trip_id_group, car_id
order by car_id, start_time;


-- Function to generate an OD matrix by finding the nearest graph vertices to trip start/end points
-- This shows how to find the network nodes closest to a given point using <-> operator
create or replace function generate_od_matrix(
    trips_table_schema text, 
    trips_table_name text, 
    vertices_table_schema text, 
    vertices_table_name text, 
    srid int
)
returns table(trip_id int, source_vertex int, target_vertex int) as $$
declare
    rec record;
    source_id int;
    target_id int;
begin
    for rec in execute format('select trip_id, geom_start, geom_end from %I.%I', trips_table_schema, trips_table_name)
    loop
        -- find nearest node for source
        execute format($f$
            select v.id 
            from %I.%I v
            join network.ways w on v.id = w.source or v.id = w.target
            where w.tag_id not in (101, 102, 103, 104, 105, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123)
            order by v.the_geom <-> st_setsrid(st_makepoint(%s, %s), %s)
            limit 1
        $f$, 
        vertices_table_schema, vertices_table_name, 
        st_x(rec.geom_start), st_y(rec.geom_start), srid
        ) into source_id;

        -- find nearest node for target
        execute format($f$
            select v.id 
            from %I.%I v
            join network.ways w on v.id = w.source or v.id = w.target
            where w.tag_id not in (101, 102, 103, 104, 105, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123)
            order by v.the_geom <-> st_setsrid(st_makepoint(%s, %s), %s)
            limit 1
        $f$, 
        vertices_table_schema, vertices_table_name, 
        st_x(rec.geom_end), st_y(rec.geom_end), srid
        ) into target_id;

        trip_id := rec.trip_id;
        source_vertex := source_id;
        target_vertex := target_id;
        return next;
    end loop;
    return;
end;
$$ language plpgsql;


-- Create OD matrix from trips
create table analysis.odm as 
select 
    trip_id,
    source_vertex as source,
    target_vertex as target
from
    generate_od_matrix('scooters', 'trips', 'network', 'ways_vertices_pgr', 28992);


-- Indexes for performance
create index idx_ways_vertices_pgr on network.ways_vertices_pgr using gist(the_geom);
create index idx_trips_start on scooters.trips using gist(geom_start);
create index idx_trips_end on scooters.trips using gist(geom_end);


-- Add columns to store the found vertices in trips table
alter table scooters.trips add column start_vid int;
alter table scooters.trips add column end_vid int;

update scooters.trips
set start_vid = odm.source
from analysis.odm odm
where scooters.trips.trip_id = odm.trip_id;


-- Running routing queries using pgr_dijkstra:
-- We pick edges that are allowed (tag_id filter) and run dijkstra on multiple OD pairs
create table analysis.route_result as
with dijkstra as (
    select *
    from pgr_dijkstra(
        'select gid as id, source, target, length_m as cost from network.ways where tag_id not in (101, 102, 103, 104, 105, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123)',
        'select source, target from analysis.odm',
        directed := false
    )
),
routes as (
    select 
        d.start_vid,
        d.end_vid,
        max(d.agg_cost) as cost_m,
        st_union(w.the_geom) as geom
    from
        dijkstra d
        inner join network.ways w on d.edge = w.gid
    group by d.start_vid, d.end_vid
)
select
    a.start_vid,
    a.end_vid,
    b.trip_id,
    b.start_time,
    b.end_time,
    b.price_cent,
    b.isdamaged,
    b.avg_max_fuellevel,
    b.avg_min_fuellevel,
    a.cost_m,
    cost_m / 1000 / extract(epoch from b.end_time - b.start_time) / 3600 as avg_speed,
    st_transform(a.geom, 4326) as geom
from
    routes a
    inner join scooters.trips b
    on a.start_vid = b.start_vid and a.end_vid = b.end_vid;

alter table analysis.route_result add column id serial;
alter table analysis.route_result add column duration interval;

update analysis.route_result
set duration = end_time - start_time;


-- ================================================================================================
-- 7. Analyzing Routes in Relation to H3 Cells
-- ================================================================================================
-- The following functions show how to intersect routes with H3 cells and gather statistics.

-- Example function that intersects routes with H3 cells and returns per-segment stats
-- This shows how to combine route geometry with H3-based cells to get contextual metrics.
create or replace function analyze_routes()
returns setof record as $$
declare
    route record;
    cell record;
    clipped_geom geometry;
    merged_geom geometry;
    individual_geom geometry;
    segment_bearing double precision;
    length double precision := 0;
    total_time interval;
    start_time timestamp;
    end_time timestamp;
    geom_dump record;
begin
    for route in (
        select * 
        from analysis.route_result 
        where analysis.route_result.end_time - analysis.route_result.start_time between '3 minutes' and '1 hour' 
          and cost_m > 250
    )
    loop
        total_time := route.end_time - route.start_time;
        start_time := route.start_time;
        end_time := route.end_time;

        for cell in select * from results.cell_9 loop
            if st_intersects(route.geom, cell.geom) then
                clipped_geom := st_intersection(route.geom, cell.geom);
                merged_geom := st_linemerge(clipped_geom);

                if st_geometrytype(merged_geom) = 'ST_LineString' then
                    segment_bearing := st_azimuth(st_startpoint(st_transform(merged_geom, 28992)), st_endpoint(st_transform(merged_geom, 28992)));
                    length := st_length(st_transform(merged_geom, 28992));

                    return query select 
                        route.trip_id,
                        segment_bearing,
                        segment_bearing * 180 / pi() as angle,
                        start_time,
                        end_time,
                        total_time,
                        length,
                        cell.index,
                        cell.bearing,
                        cell.angle,
                        cell.density,
                        cell.connectivity,
                        cell.bicycle_lane_density,
                        cell.primary_density,
                        cell.secondary_density,
                        cell.other_density,
                        cell.ped_density,
                        cell.dead_end_density,
                        merged_geom;
                else
                    -- Multiple line segments inside the cell
                    for geom_dump in select (st_dump(merged_geom)).geom loop
                        if st_geometrytype(geom_dump.geom) = 'ST_LineString' then
                            individual_geom := geom_dump.geom;
                            segment_bearing := st_azimuth(st_startpoint(st_transform(individual_geom, 28992)), st_endpoint(st_transform(individual_geom, 28992)));
                            length := st_length(st_transform(individual_geom, 28992));

                            return query select 
                                route.trip_id,
                                segment_bearing,
                                segment_bearing * 180 / pi() as angle,
                                start_time,
                                end_time,
                                total_time,
                                length,
                                cell.index,
                                cell.bearing,
                                cell.angle,
                                cell.density,
                                cell.connectivity,
                                cell.bicycle_lane_density,
                                cell.primary_density,
                                cell.secondary_density,
                                cell.other_density,
                                cell.ped_density,
                                cell.dead_end_density,
                                individual_geom;
                        end if;
                    end loop;
                end if;
            end if;
        end loop;
    end loop;
end;
$$ language plpgsql;


create table results.cell_9_routes as (
    select * from analyze_routes() as (
        trip_id INT,
        segment_bearing DOUBLE PRECISION,
        angle DOUBLE PRECISION,
        start_time timestamp,
        end_time timestamp,
        total_time interval,
        length double precision,
        cell_index TEXT,
        cell_bearing DOUBLE PRECISION,
        cell_angle DOUBLE PRECISION,
        cell_density DOUBLE PRECISION,
        cell_connectivity DOUBLE PRECISION,
        cell_bicycle_lane_density DOUBLE PRECISION,
        cell_primary_density DOUBLE PRECISION,
        cell_secondary_density DOUBLE PRECISION,
        cell_other_density DOUBLE PRECISION,
        cell_ped_density DOUBLE PRECISION,
        cell_dead_end_density DOUBLE PRECISION,
        segment_geom GEOMETRY
    )
);


-- Another variant of the route analysis function to run in parallel or in batches
create or replace function analyze_routes_parallel(start_id int, end_id int)
returns table(
    trip_id int,
    segment_bearing double precision,
    angle double precision,
    start_time timestamp,
    end_time timestamp,
    total_time interval,
    length double precision,
    cell_index text,
    cell_bearing double precision,
    cell_angle double precision,
    cell_density double precision,
    cell_connectivity double precision,
    cell_bicycle_lane_density double precision,
    cell_primary_density double precision,
    cell_secondary_density double precision,
    cell_other_density double precision,
    cell_ped_density double precision,
    cell_dead_end_density double precision,
    segment_geom geometry
) as $$
declare
    route record;
    cell record;
    clipped_geom geometry;
    merged_geom geometry;
    individual_geom geometry;
    segment_bearing double precision;
    length double precision := 0;
    total_time interval;
    start_time timestamp;
    end_time timestamp;
    geom_dump record;
begin
    for route in 
        select * from analysis.route_result 
        where id >= start_id and id <= end_id
        and duration between '3 minutes' and '1 hour' 
        and cost_m > 250
    loop
        total_time := route.end_time - route.start_time;
        start_time := route.start_time;
        end_time := route.end_time;

        for cell in select * from results.cell_9 loop
            if st_intersects(route.geom, cell.geom) then
                clipped_geom := st_intersection(route.geom, cell.geom);
                merged_geom := st_linemerge(clipped_geom);

                if st_geometrytype(merged_geom) = 'ST_LineString' then
                    segment_bearing := st_azimuth(st_startpoint(st_transform(merged_geom, 28992)), st_endpoint(st_transform(merged_geom, 28992)));
                    length := st_length(st_transform(merged_geom, 28992));

                    return query select 
                        route.trip_id,
                        segment_bearing,
                        segment_bearing * 180 / pi() as angle,
                        start_time,
                        end_time,
                        total_time,
                        length,
                        cell.index,
                        cell.bearing,
                        cell.angle,
                        cell.density,
                        cell.connectivity,
                        cell.bicycle_lane_density,
                        cell.primary_density,
                        cell.secondary_density,
                        cell.other_density,
                        cell.ped_density,
                        cell.dead_end_density,
                        merged_geom;
                else
                    for geom_dump in select (st_dump(merged_geom)).geom loop
                        if st_geometrytype(geom_dump.geom) = 'ST_LineString' then
                            individual_geom := geom_dump.geom;
                            segment_bearing := st_azimuth(st_startpoint(st_transform(individual_geom, 28992)), st_endpoint(st_transform(individual_geom, 28992)));
                            length := st_length(st_transform(individual_geom, 28992));

                            return query select 
                                route.trip_id,
                                segment_bearing,
                                segment_bearing * 180 / pi() as angle,
                                start_time,
                                end_time,
                                total_time,
                                length,
                                cell.index,
                                cell.bearing,
                                cell.angle,
                                cell.density,
                                cell.connectivity,
                                cell.bicycle_lane_density,
                                cell.primary_density,
                                cell.secondary_density,
                                cell.other_density,
                                cell.ped_density,
                                cell.dead_end_density,
                                individual_geom;
                        end if;
                    end loop;
                end if;
            end if;
        end loop;
    end loop;
end;
$$ language plpgsql;


-- Example query to run the parallel analysis for a subset of route IDs
-- SELECT * FROM analyze_routes_parallel(1, 10);


-- Create a table to store route-cell stats (if desired)
create table results.route_cell_stats_9 (
    trip_id int,
    segment_bearing double precision,
    angle double precision,
    start_time timestamp,
    end_time timestamp,
    total_time interval,
    length double precision,
    cell_index text,
    cell_bearing double precision,
    cell_angle double precision,
    cell_density double precision,
    cell_connectivity double precision,
    cell_bicycle_lane_density double precision,
    cell_primary_density double precision,
    cell_secondary_density double precision,
    cell_other_density double precision,
    cell_ped_density double precision,
    cell_dead_end_density double precision,
    segment_geom geometry
);


-- Normalize angles if needed
alter table results.route_cell_stats_9 add column cell_angle_normalized double precision;

update results.route_cell_stats_9
set angle_normalized = case
    when angle > 180 then angle - 180
    else angle
end;


-- Compute cell-level averages of route stats
create table results.route_cell_averages as (
select
    cell_index h3_index,
    avg(angle_normalized) mean_segment_angle,
    avg(length) mean_length,
    sum(length) sum_length,
    count(cell_index) segment_count,
    cell_angle_normalized cell_angle,
    cell_bicycle_lane_density,
    cell_primary_density,
    cell_secondary_density,
    cell_other_density,
    cell_ped_density,
    cell_dead_end_density,
    st_setsrid(h3_cell_to_boundary(cell_index::h3index)::geometry, 4326) as geom
from
    results.route_cell_stats_9
group by
    cell_index,
    cell_angle_normalized,
    cell_bicycle_lane_density,
    cell_primary_density,
    cell_secondary_density,
    cell_other_density,
    cell_ped_density,
    cell_dead_end_density
);


-- Indices for analysis tables
-- On analysis.route_result
create index idx_route_result_id on analysis.route_result(id);
create index idx_route_result_duration on analysis.route_result(duration);
create index idx_route_result_cost_m on analysis.route_result(cost_m);
create index idx_route_result_geom on analysis.route_result using gist(geom);

-- On results.cell_9
create index idx_cell_9_geom on results.cell_9 using gist(geom);


-- ================================================================================================
-- End of file.
-- ================================================================================================
