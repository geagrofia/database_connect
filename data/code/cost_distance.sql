-- example: select COVERAGE.COST_DISTANCE()
-- function cost_distance


-- this function will iterate through all the potential ag dealers and calculate the population within
-- 15km (the maximum distance by bicycle within 1 hour - used as a filter) and within
-- 1 hour along the friction_b network
-- the ag dealer with the greatest combined population will be chosen
-- the population will be removed
-- the population target will be assessed
-- the function will move on to the next iteration

CREATE OR REPLACE FUNCTION COVERAGE.COST_DISTANCE()
RETURNS INTEGER AS
$$ -- dollar quotes
DECLARE
  _iteration INTEGER :=0; -- iteration number
  _curs REFCURSOR; -- cursor
  _rec RECORD;
  _endvid_count INTEGER :=0; -- number of valid population targets for each agro-dealer
  _pophour NUMERIC :=0; -- population within 1 hour of each agro-dealer
  _pophour_max NUMERIC :=0; -- maximum population within 1 hour of every agro-dealer per iteration
  _pophour_max_id INTEGER; -- agro-dealer with maximum population within 1 hour per iteration
  _pophour_total INTEGER :=0; -- total population covered by the selected agro-dealers
  _pop_target INTEGER :=3292660; -- target population that needs to be covered by the selected agro-dealers
  _TEMP_TABNAME text; -- 	to avoid transation lock problems the temporary tables will be renamed using this text
  _query text ; -- need to execute this query string



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


-- notify
RAISE NOTICE 'COST_DISTANCE: begun.';

<<POP_TOTAL_LOOP>>
WHILE _pophour_total < _pop_target LOOP

_iteration := _iteration + 1;

-- open cursor, with a record for each potential agro-dealer

  OPEN _curs FOR SELECT Id, pop_remain, ag_new_id, the_geom
                 FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR
                 WHERE the_geom IS NOT NULL AND ag_new_id IS NOT NULL
                 ORDER BY Id ASC;

    <<AG_DEALER_LOOP>>
  	LOOP

			-- get record
  		FETCH NEXT FROM _curs INTO _rec;
  		EXIT WHEN NOT FOUND;

		  -- notify
			RAISE NOTICE 'COST_DISTANCE - Current value of _Id (%)', _rec.Id;
			RAISE NOTICE 'COST_DISTANCE - Current value of ag_new_id (%)', _rec.ag_new_id;

--THIS IS AN OLD SECTION  03/03/2022

			-- targets are the population points
			DROP TABLE  IF EXISTS end_vids CASCADE;
			CREATE TEMP TABLE end_vids (target   integer );

			-- only select those with a population above 0 and that are within 15 kilometres
			INSERT INTO end_vids
			(SELECT V.id
			FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
			WHERE V.pop_remain > 0 AND ST_DISTANCE(ST_TRANSFORM(_rec.the_geom, 32736 ), ST_TRANSFORM(V.the_geom, 32736)) < 15000);

--END OF AN OLD SECTION  03/03/2022

--THIS IS A NEW SECTION COMMENTED OUT 03/03/2022
/*
			_TEMP_TABNAME := CONCAT('TEMP_TABLE_', _rec.Id );
      RAISE NOTICE '_TEMP_TABNAME = (%)', _TEMP_TABNAME;

  		_query := format('drop table if exists %I', _TEMP_TABNAME);
  		RAISE NOTICE '_query = (%)', _query;
  		EXECUTE _query; 

  		_query := format('create temp table  %I (target integer )', _TEMP_TABNAME); 
  		RAISE NOTICE '_query = (%)', _query;
  		EXECUTE _query;

  		_query := format('INSERT INTO %I'
  			'(SELECT V.id
			FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
			WHERE V.pop_remain > 0 AND ST_DISTANCE(ST_TRANSFORM(_rec.the_geom, 32736 ), ST_TRANSFORM(V.the_geom, 32736)) < 15000)', _TEMP_TABNAME);
  		RAISE NOTICE '_query = (%)', _query;
  		EXECUTE _query;

			EXIT AG_DEALER_LOOP; 
*/
--END OF THE NEW COMMENTED OUT SECTION 03/03/2022

			-- debug - get number of records in the table
			_endvid_count := (SELECT COUNT(*) FROM end_vids);
			RAISE NOTICE 'COST_DISTANCE - _endvid_count (%)', _endvid_count;

			-- cross join end_vids with the record Id
			-- this table needs to have two columns 'source' with the record number 
			-- and 'target' with the population vertex number from 'endvid'

			DROP TABLE  IF EXISTS COVERAGE.start_end_vids CASCADE;
			CREATE TABLE COVERAGE.start_end_vids AS (
    		SELECT V.Id source, E.target
				FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
				CROSS JOIN end_vids E
				WHERE V.Id = _rec.Id);

			-- paths are the shortest paths
			DROP TABLE IF EXISTS AGG_COST_TABLE CASCADE;
			CREATE TEMP TABLE AGG_COST_TABLE (start_vid integer, end_vid integer, agg_cost  numeric(10,2), pophour numeric(10,2));

			INSERT INTO AGG_COST_TABLE
				(SELECT A.start_vid, A.end_vid, A.agg_cost, V.pop_remain pophour
				FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 
													'SELECT * FROM COVERAGE.start_end_vids') A, --pgr_dijkstra expects node ids
						COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
				WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.end_vid);

			-- debug - get population count in the table
			_pophour := (SELECT SUM(pophour) FROM AGG_COST_TABLE);
			RAISE NOTICE 'COST_DISTANCE - _pophour (%)', _pophour;

			IF _pophour > _pophour_max THEN
				_pophour_max := _pophour;
				_pophour_max_id := _rec.Id;
				RAISE NOTICE 'COST_DISTANCE - _pophour_max (%)', _pophour_max;
				RAISE NOTICE 'COST_DISTANCE - _pophour_max_id (%)', _pophour_max_id;
			END IF;

		END LOOP;

		--EXIT POP_TOTAL_LOOP; --new line commented out 03/03/2022

		RAISE NOTICE 'COST_DISTANCE - _pophour_max (%)', _pophour_max;
		RAISE NOTICE 'COST_DISTANCE - _pophour_max_id (%)', _pophour_max_id;

   -- Close the cursor
   CLOSE _curs;
  
    -- repeat for the ag_dealer with the greatest population within 1 hour

   -- source is the ag_dealer point
			DROP TABLE  IF EXISTS start_vids CASCADE;
			CREATE TEMP TABLE start_vids (source   integer, 
																		the_geom GEOMETRY(POINT, 4326) );

			INSERT INTO start_vids
			(SELECT V.id source, V.the_geom
			FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
			WHERE V.id = _pophour_max_id);

   -- targets are the population points
			DROP TABLE  IF EXISTS end_vids CASCADE;
			CREATE TEMP TABLE end_vids (target   integer );

			-- only select those with a population above 0 and that are within 15 kilometres
			INSERT INTO end_vids
			(SELECT V.id
			FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V, start_vids S
			WHERE V.pop_remain > 0 AND ST_DISTANCE(ST_TRANSFORM(S.the_geom, 32736 ), ST_TRANSFORM(V.the_geom, 32736)) < 15000);

			-- debug - get number of records in the table
			_endvid_count := (SELECT COUNT(*) FROM end_vids);
			RAISE NOTICE 'COST_DISTANCE - _endvid_count (%)', _endvid_count;

			-- cross join end_vids with the record Id
			-- this table needs to have two columns 'source' with the record number 
			-- and 'target' with the population vertex number from 'endvid'

			-- first need to delete the_geom column from start_vids this is no longer needed
    	ALTER TABLE start_vids 	DROP COLUMN IF EXISTS the_geom;
			RAISE NOTICE 'COST_DISTANCE - GOT THIS FAR 1';

			DROP TABLE IF EXISTS COVERAGE.start_end_vids CASCADE;
			CREATE TABLE COVERAGE.start_end_vids AS (
    		SELECT V.source, E.target
				FROM start_vids V
				CROSS JOIN end_vids E);

			RAISE NOTICE 'COST_DISTANCE - GOT THIS FAR 2';

			-- paths are the shortest paths
			DROP TABLE IF EXISTS AGG_COST_TABLE CASCADE;
			CREATE TEMP TABLE AGG_COST_TABLE (start_vid integer, end_vid integer, agg_cost  numeric(10,2), pophour numeric(10,2));

			RAISE NOTICE 'COST_DISTANCE - GOT THIS FAR 3';

			INSERT INTO AGG_COST_TABLE
				(SELECT A.start_vid, A.end_vid, A.agg_cost, V.pop_remain pophour
				FROM pgr_dijkstra('SELECT * FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED', 
													'SELECT * FROM COVERAGE.start_end_vids') A, --pgr_dijkstra expects node ids
						COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V
				WHERE A.edge = -1 AND A.agg_cost <3600 AND V.Id = A.end_vid);

			-- debug - get population count in the table
			_pophour := (SELECT SUM(pophour) FROM AGG_COST_TABLE);
			RAISE NOTICE 'COST_DISTANCE - _pophour (%)', _pophour;

			--get the population covered and save as a permanent table for debugging

			RAISE NOTICE 'COST_DISTANCE - GOT THIS FAR 4';

			DROP TABLE IF EXISTS COVERAGE.POP_COVERED CASCADE;
			CREATE TABLE COVERAGE.POP_COVERED AS (
    		SELECT V.Id, V.the_geom 
				FROM COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR V, AGG_COST_TABLE E
				WHERE V.Id = E.end_vid);

			_pophour_total := _pophour_total + _pophour;
			RAISE NOTICE 'COST_DISTANCE - _pophour_total (%)', _pophour_total;

			--remove the covered population from the nodes

			UPDATE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR SET pop_remain = 0, iteration = _iteration
			FROM COVERAGE.POP_COVERED A
			WHERE COVERAGE.FRICTION_B_LINES_COMBINED_NO_DUPES_NODED_VERTICES_PGR.Id = A.Id;

			-- reset _pophour_max to 0
			_pophour_max := 0;

			--remove the end_vids table
			DROP TABLE  IF EXISTS end_vids CASCADE;

	END LOOP;


-- notify
RAISE NOTICE 'COST_DISTANCE: completed successfully.';
RETURN 0;

END;
$$
LANGUAGE plpgsql;