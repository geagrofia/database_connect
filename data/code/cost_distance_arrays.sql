DROP FUNCTION COVERAGE.ARRAYS_4_PGRDIJKSTRA();
CREATE OR REPLACE FUNCTION COVERAGE.ARRAYS_4_PGRDIJKSTRA()
--RETURNS TABLE (TARGET BIGINT) AS
--RETURNS TEXT AS
--RETURNS TABLE (start_vid bigint, end_vid bigint, agg_cost float, pophour int) AS
RETURNS INTEGER AS

$$ -- dollar quotes
DECLARE
  _iteration INTEGER :=0; -- iteration number
  _curs REFCURSOR; -- cursor
  _rec RECORD;
  _SOURCE BIGINT ARRAY;
  _SOURCE_length INTEGER; -- length of source array for debugging
  _TARGET BIGINT ARRAY;
  _TARGET_length INTEGER; -- length of target array for debugging
  _TABNAME text;
  _TARGET_ARRAY text;
  _SOURCE_ARRAY text;
  _query_pop text ; -- need to execute this query string to populate agg_cost_pop array
  _query_id text ; -- need to execute this query string to populate agg_cost_id array
  _AGG_COST_POP int ARRAY; -- stores population of points within 1 hour of each ag-dealer
  _AGG_COST_ID int ARRAY;  -- stores id of points within 1 hour of each agro-dealer
  _pophour NUMERIC :=0; -- population within 1 hour of each agro-dealer
  _ag_dealer_pop NUMERIC :=0; -- population of the selected agro-dealer location
  _pophour_max NUMERIC :=0; -- maximum population within 1 hour of every agro-dealer per iteration
  _pophour_max_id INTEGER; -- agro-dealer with maximum population within 1 hour per iteration
  _pophour_total INTEGER :=0; -- total population covered by the selected agro-dealers
  _pop_target INTEGER :=3292660; -- target population that needs to be covered by the selected agro-dealers

BEGIN

-- add a new column to the vertices of the network for the iteration that the population is added
-- the default value is NULL

ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR DROP COLUMN IF EXISTS iteration;
ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR ADD COLUMN iteration INTEGER;

-- add a new column to the vertices of the network for the population that is no longer covered 
-- the default value is the original population (pop_b)

ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR DROP COLUMN IF EXISTS pop_remain;
ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR ADD COLUMN pop_remain INTEGER;

UPDATE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR SET pop_remain = pop_b;

-- add a new column to the vertices of the network for potential agdealers that have yet to be added to the network 
-- the default value is 0, when they are added the value changes to 1

ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR DROP COLUMN IF EXISTS ag_new_chosen;
ALTER TABLE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR ADD COLUMN ag_new_chosen INTEGER default 0;


<<POP_TOTAL_LOOP>>
WHILE _pophour_total < _pop_target LOOP
_iteration := _iteration + 1;

-- OPEN CURSOR AND LOOP TO POPULATE ARRAY

-- open cursor, with a record for each potential agro-dealer

  OPEN _curs FOR SELECT Id, pop_remain, ag_new_id, the_geom
                 FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR
                 WHERE the_geom IS NOT NULL AND ag_new_id IS NOT NULL AND iteration IS NULL AND ag_new_chosen = 0
                 ORDER BY Id ASC;

    <<AG_DEALER_LOOP>>
    LOOP

        -- get record
        FETCH NEXT FROM _curs INTO _rec;
        EXIT WHEN NOT FOUND;

        -- get the source population nodes
        _SOURCE := ARRAY( SELECT V.id
                      FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
                      WHERE V.pop_remain > 0 AND ST_DISTANCE(ST_TRANSFORM(_rec.the_geom, 32736 ), ST_TRANSFORM(V.the_geom, 32736)) < 15000);

        -- get the target agro-dealer
        _TARGET := ARRAY[]::integer[];
        _TARGET := ARRAY_APPEND(_TARGET, _rec.Id);

        --format the source and target arrays for use in the pgrdijkstra function
        _SOURCE_ARRAY := CONCAT('ARRAY', format_array( _SOURCE) );
        _TARGET_ARRAY := CONCAT('ARRAY', format_array( _TARGET) );

        --construct the query for running the pgrdijkstra function using the source and target arrays
        _query_pop := 'SELECT ARRAY(SELECT V.pop_remain pophour FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'', ARRAY' 
        || format_array( _SOURCE)||
        ', ARRAY'
        || format_array( _TARGET)||
        ') A, COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.start_vid)';

        -- execute the query and send results to population array
        EXECUTE _query_pop INTO _AGG_COST_POP;

        -- get population count in the array and save as variable value
        _pophour := (SELECT SUM(A) FROM UNNEST(_AGG_COST_POP) AS A);
        --RAISE NOTICE 'COST_DISTANCE - _pophour (%)', _pophour;
        
        -- population of the ag-dealer location is not counted in the pgrdijkstra function

        _ag_dealer_pop :=   (SELECT pop_remain 
                            FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR A
                            WHERE A.Id = _rec.Id);
        --RAISE NOTICE 'COST_DISTANCE - _ag_dealer_pop (%)', _ag_dealer_pop;

        IF _ag_dealer_pop > 0 THEN
            -- add population of the ag-dealer location to the other locatiions
            _pophour := (_pophour + _ag_dealer_pop);
        END IF;

        --RAISE NOTICE 'COST_DISTANCE - _pophour (%)', _pophour;

        -- for each record compare to max pop
        IF _pophour > _pophour_max THEN
                _pophour_max := _pophour;
                _pophour_max_id := _rec.Id;
                RAISE NOTICE 'COST_DISTANCE - _pophour_max (%)', _pophour_max;
                RAISE NOTICE 'COST_DISTANCE - _pophour_max_id (%)', _pophour_max_id;
                _SOURCE_length := array_length(_SOURCE, 1);--debug
                --RAISE NOTICE 'COST_DISTANCE - _SOURCE_length (%)', _SOURCE_length; --debug - what is the source length - does it match the source length above
                --RAISE NOTICE 'COST_DISTANCE - _SOURCE_ARRAY (%)', _SOURCE_ARRAY;--debug
                --RAISE NOTICE 'COST_DISTANCE - _AGG_COST_POP (%)', _AGG_COST_POP;--debug

        END IF;

    END LOOP;  -- end <<AG_DEALER_LOOP>>

   -- Close the cursor
   CLOSE _curs;

-- repeat for the ag_dealer with the greatest population within 1 hour

    --RAISE NOTICE 'COST_DISTANCE BETWEEN LOOPS - _pophour_max (%)', _pophour_max; --debug - information from previous loop
    --RAISE NOTICE 'COST_DISTANCE BETWEEN LOOPS - _pophour_max_id (%)', _pophour_max_id; --debug - information from previous loop

-- OPEN CURSOR AND LOOP ONCE TO RUN PRDIJKSTRA ON AGRO-DEALER WITH MAX POP

-- open cursor, with a record for agro-dealer with max pop

  OPEN _curs FOR SELECT Id, pop_remain, ag_new_id, the_geom
                 FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR
                 WHERE Id = _pophour_max_id;

    <<AG_DEALER_MAX_POP_LOOP>>
    LOOP
                
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _pophour_max (%)', _pophour_max; --debug - information from previous loop
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _pophour_max_id (%)', _pophour_max_id; --debug - information from previous loop

        -- get record
        FETCH NEXT FROM _curs INTO _rec;
        EXIT WHEN NOT FOUND;

        -- target is the ag_dealer point
        _TARGET := ARRAY[]::integer[];
        _TARGET := ARRAY_APPEND(_TARGET, _rec.Id);
        _TARGET_length := array_length(_TARGET, 1);

        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _rec.Id (%)', _rec.Id; --debug - what is the record Id - does it match the _pophour_max_id
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _TARGET_length (%)', _TARGET_length; --debug - what is the target length - does it match the target length above

        -- sources are the population points

        -- get the source population nodes
        _SOURCE := ARRAY( SELECT V.id
                      FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
                      WHERE V.pop_remain > 0 AND ST_DISTANCE(ST_TRANSFORM(_rec.the_geom, 32736 ), ST_TRANSFORM(V.the_geom, 32736)) < 15000);

        _SOURCE_length := array_length(_SOURCE, 1);
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _SOURCE_length (%)', _SOURCE_length; --debug - what is the source length - does it match the source length above

       --format the source and target arrays for use in the pgrdijkstra function
        _SOURCE_ARRAY := CONCAT('ARRAY', format_array( _SOURCE) );
        _TARGET_ARRAY := CONCAT('ARRAY', format_array( _TARGET) );
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _SOURCE_ARRAY (%)', _SOURCE_ARRAY;--debug

        --construct the query for running the pgrdijkstra function using the source and target arrays
        _query_pop := 'SELECT ARRAY(SELECT V.pop_remain pophour FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'', ARRAY' 
        || format_array( _SOURCE)||
        ', ARRAY'
        || format_array( _TARGET)||
        ') A, COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.start_vid)';

        -- execute the query and send results to population array
        EXECUTE _query_pop INTO _AGG_COST_POP;
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _query_pop (%)', _query_pop;--debug
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _AGG_COST_POP (%)', _AGG_COST_POP;--debug

        -- get population count in the array and save as variable value
        _pophour := (SELECT SUM(A) FROM UNNEST(_AGG_COST_POP) AS A);
        --RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _pophour (%)', _pophour;

        -- population of the ag-dealer location is not counted in the pgrdijkstra function

        _ag_dealer_pop :=   (SELECT pop_remain 
                            FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR A
                            WHERE A.Id = _rec.Id);
        --RAISE NOTICE 'COST_DISTANCE - _ag_dealer_pop (%)', _ag_dealer_pop;

        IF _ag_dealer_pop > 0 THEN
            -- add population of the ag-dealer location to the other locatiions
            _pophour := (_pophour + _ag_dealer_pop);
        END IF;

        RAISE NOTICE 'COST_DISTANCE - _pophour (%)', _pophour;

       --construct the query for running the pgrdijkstra function using the source and target arrays
        _query_id := 'SELECT ARRAY(SELECT V.Id FROM pgr_dijkstra(''SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED'', ARRAY' 
        || format_array( _SOURCE)||
        ', ARRAY'
        || format_array( _TARGET)||
        ') A, COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.start_vid)';

        -- execute the query and send results to population id array
        EXECUTE _query_id INTO _AGG_COST_ID;

        -- add the ag-dealer location to the other locations
        _AGG_COST_ID = ARRAY_APPEND(_AGG_COST_ID, _rec.Id);

        --get the population covered and save as a permanent table for debugging
        DROP TABLE IF EXISTS COVERAGE.POP_COVERED CASCADE;
        CREATE TABLE COVERAGE.POP_COVERED AS (
        SELECT V.Id, V.the_geom 
            FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
            WHERE V.Id IN (SELECT E FROM
                   UNNEST(_AGG_COST_ID) G(E)));


        _pophour_total := _pophour_total + _pophour;
        RAISE NOTICE 'COST_DISTANCE MAX POP LOOP - _pophour_total (%)', _pophour_total;

        --remove the covered population from the nodes

        UPDATE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR SET pop_remain = 0, iteration = _iteration, ag_new_chosen = 1
        FROM COVERAGE.POP_COVERED A
        WHERE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR.Id = A.Id;

        -- reset _pophour_max to 0
        _pophour_max := 0;

    END LOOP; -- end <<AG_DEALER_MAX_POP_LOOP>>

   -- Close the cursor
   CLOSE _curs;

END LOOP; -- end <<POP_TOTAL_LOOP>>

RETURN 0;

END
$$
LANGUAGE plpgsql;
