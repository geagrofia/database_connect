-- 1 create vertices of the network

--select pgr_nodenetwork('COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES', 0.000001, 'comb_id', 'geom', 'noded')

-- 2 create topology of the network

--select pgr_createTopology('COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 0.000001, 'geom', 'id', 'source', 'target' )


-- 3 add cost and reverse_cost to complete the network

--ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED ADD COLUMN cost numeric(10,2);

--UPDATE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED 
--SET cost = B.cost 
--FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES B
--WHERE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED.old_id = B.comb_id;

--ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED ADD COLUMN reverse_cost numeric(10,2);

--UPDATE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED 
--SET reverse_cost = B.cost 
--FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED B
--WHERE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED.old_id = B.old_id

-- 4 analyse graph
--select pgr_analyzegraph('COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 0.000001,'geom','id','source','target','true');


-- 5 one-to-one with manually filled array

--select * FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 1, 45797)


-- 6 one-to-one with manually filled array gets geometry of path

--WITH PATH AS(
--select * FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 1, 45797))
--	SELECT * 
--	FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED A, PATH B
--	WHERE A.id = B.edge;


-- 7 many-to-many with manually filled arrays

--WITH PATH AS(
--select * FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', ARRAY[1, 5], ARRAY[38765, 45797])) --pgr_dijkstra expects node ids
--	SELECT start_vid, end_vid, agg_cost
--	FROM  PATH 
--	WHERE edge = -1;


-- 8 manually filled one-to-one combinations table

--DROP TABLE  IF EXISTS start_end_vids CASCADE;
--CREATE TEMP TABLE start_end_vids (
--    source  integer,
--    target   integer 
--);

--INSERT INTO start_end_vids
--    VALUES (7, 30007);
--INSERT INTO start_end_vids
--    VALUES (6, 30006);	

--WITH PATH AS(
--select * FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 'SELECT * FROM start_end_vids')) --pgr_dijkstra expects node ids
--	SELECT start_vid, end_vid, agg_cost
--	FROM  PATH 
--	WHERE edge = -1;
	
	
-- 9 cross join to populate one-to-one combinations table
/*
-- sources are the potential agro dealers
DROP TABLE  IF EXISTS start_vids CASCADE;
CREATE TEMP TABLE start_vids (
    source  integer 
);	

INSERT INTO start_vids
(SELECT id
FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR
WHERE ag_new_id IS NOT NULL);

--SELECT *
--FROM start_vids;

-- targets are the population points
DROP TABLE  IF EXISTS end_vids CASCADE;
CREATE TEMP TABLE end_vids (
    target   integer 
);


-- only select
INSERT INTO end_vids
(SELECT id
FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR
WHERE pop_b > 0);

--SELECT *
--FROM end_vids;


-- cross join only when end_vids are within 15km of start_vids
DROP TABLE  IF EXISTS COVERAGE.start_end_vids CASCADE;
CREATE TABLE COVERAGE.start_end_vids AS (
    SELECT *
FROM start_vids
CROSS JOIN end_vids 
);

--SELECT *
--FROM start_end_vids;

WITH PATH AS(
select * FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 'SELECT * FROM COVERAGE.start_end_vids')) --pgr_dijkstra expects node ids
	SELECT start_vid, end_vid, agg_cost
	FROM  PATH 
	WHERE edge = -1 AND agg_cost <3600;

*/

-- 10 arrays to populate one-to-many routing with arrays

DROP FUNCTION COVERAGE.ARRAYS_4_PGRDIJKSTRA();
CREATE OR REPLACE FUNCTION COVERAGE.ARRAYS_4_PGRDIJKSTRA()
--RETURNS TABLE (TARGET BIGINT) AS
--RETURNS TEXT AS
--RETURNS TABLE (start_vid bigint, end_vid bigint, agg_cost float, pophour int) AS
RETURNS INTEGER AS

$$ -- dollar quotes
DECLARE
  _curs REFCURSOR; -- cursor
  _rec RECORD;
  _SOURCE BIGINT ARRAY;
  _TARGET BIGINT ARRAY;
  _TABNAME text;
  _TARGET_ARRAY text;
  _SOURCE_ARRAY text;
  _query text ; -- need to execute this query string 
  _AGG_COST int ARRAY;  
  _pophour NUMERIC :=0; -- population within 1 hour of each agro-dealer
  _pophour_max NUMERIC :=0; -- maximum population within 1 hour of every agro-dealer per iteration
  _pophour_max_id INTEGER; -- agro-dealer with maximum population within 1 hour per iteration
  _pophour_total INTEGER :=0; -- total population covered by the selected agro-dealers
  _pop_target INTEGER :=3292660; -- target population that needs to be covered by the selected agro-dealers

BEGIN

-- OPEN CURSOR AND LOOP TO POPULATE ARRAY

-- open cursor, with a record for each potential agro-dealer

  OPEN _curs FOR SELECT Id, pop_remain, ag_new_id, the_geom
                 FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR
                 WHERE the_geom IS NOT NULL AND ag_new_id IS NOT NULL
                 ORDER BY Id ASC;

    LOOP

            -- get record
        FETCH NEXT FROM _curs INTO _rec;
        EXIT WHEN NOT FOUND;

        _SOURCE := ARRAY( SELECT V.id
                      FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
                      WHERE V.pop_remain > 0 AND ST_DISTANCE(ST_TRANSFORM(_rec.the_geom, 32736 ), ST_TRANSFORM(V.the_geom, 32736)) < 15000);
        
        --_TARGET := ARRAY[ 489 ];

        --_TARGET := format('ARRAY{ %s }', 489);

        _TARGET := ARRAY[]::integer[];
        _TARGET := ARRAY_APPEND(_TARGET, _rec.Id);
        --_TABNAME = CONCAT('TABLE', _rec.Id );

        --RAISE NOTICE '_TABNAME = (%)', _TABNAME;

    END LOOP;


--_SOURCE = ARRAY_APPEND(_SOURCE, 1);

RAISE NOTICE '_SOURCE = (%)', _SOURCE;

_SOURCE_ARRAY := CONCAT('ARRAY', format_array( _SOURCE) );

RAISE NOTICE '_SOURCE_ARRAY = (%)', _SOURCE_ARRAY;

RAISE NOTICE '_TARGET = (%)', _TARGET;
--RETURN format_array( _TARGET);

_TARGET_ARRAY := CONCAT('ARRAY', format_array( _TARGET) );

RAISE NOTICE '_TARGET_ARRAY = (%)', _TARGET_ARRAY;

/*
_query := format('select * FROM pgr_dijkstra(SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED, %2$I, %1$I)', _SOURCE_ARRAY, _TARGET_ARRAY);
RAISE NOTICE '_query 1 = (%)', _query;


--NEED TO ESCAPE ORIGINAL SINGLE QUOTES

--EXECUTE _query; 


_query := 'SELECT * FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'',' 
|| quote_ident(_SOURCE_ARRAY)||
','
|| quote_ident(_TARGET_ARRAY)||
')';
RAISE NOTICE '_query 2 = (%)', _query;

_query := 'SELECT * FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'',' 
|| quote_literal(_SOURCE_ARRAY)||
','
|| quote_literal(_TARGET_ARRAY)||
')';

RAISE NOTICE '_query 3 = (%)', _query;

_query := 'SELECT A.start_vid, A.end_vid, A.agg_cost, V.pop_remain pophour FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'', ARRAY' 
|| format_array( _SOURCE)||
', ARRAY'
|| format_array( _TARGET)||
') A, COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.start_vid';

RAISE NOTICE '_query 4 = (%)', _query;

_query := '_AGG_COST:= ARRAY(SELECT A.agg_cost FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'', ARRAY' 
|| format_array( _SOURCE)||
', ARRAY'
|| format_array( _TARGET)||
') A, COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.start_vid)';

RAISE NOTICE '_query 5 = (%)', _query;
*/

_query := 'SELECT ARRAY(SELECT V.pop_remain pophour FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'', ARRAY' 
|| format_array( _SOURCE)||
', ARRAY'
|| format_array( _TARGET)||
') A, COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.start_vid)';

RAISE NOTICE '_query 6 = (%)', _query;

EXECUTE _query INTO _AGG_COST;
RAISE NOTICE '_AGG_COST = (%)', _AGG_COST;

-- debug - get population count in the table
_pophour := (SELECT SUM(A) FROM UNNEST(_AGG_COST) AS A);
RAISE NOTICE 'COST_DISTANCE - _pophour (%)', _pophour;

--RETURN QUERY EXECUTE _query; 
RETURN 0;

END
$$
LANGUAGE plpgsql;

/*
WITH PATH AS(
select * FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', ARRAY[1, 5], ARRAY[38765, 45797])) --pgr_dijkstra expects node ids
  SELECT start_vid, end_vid, agg_cost
  FROM  PATH 
  WHERE edge = -1;