DROP TABLE  IF EXISTS COVERAGE.POT_AG_DEALERS_SNAP CASCADE;
CREATE TABLE IF NOT EXISTS COVERAGE.POT_AG_DEALERS_SNAP AS

SELECT
  row_number() OVER () AS id,
  ST_Transform(ST_Snap(a.geom, ST_TRANSFORM(b.the_geom, 32736), ST_Distance(a.geom, ST_TRANSFORM(b.the_geom, 32736)) * 1.01 ), 4326) 
    AS moved_geom,
  ST_Distance(a.geom, ST_TRANSFORM(b.the_geom, 32736)) 
    AS distance,
  pot_ag_dealer_name,
  a.id AS pot_ag_dealer_id,
  b.id AS near_node_id
FROM COVERAGE.POT_AG_DEALERS AS a
CROSS JOIN LATERAL (
  SELECT nodes.the_geom, nodes.id
  FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR nodes
  --WHERE a.id != nodes.id
  ORDER BY
    a.geom <-> ST_TRANSFORM(nodes.the_geom, 32736)
  LIMIT 1) AS b