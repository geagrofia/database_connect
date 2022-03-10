-- example: select COVERAGE.CONNECT_FOUR_NEIGHBOURS_ORDINAL_NODUPES(4)

CREATE OR REPLACE FUNCTION COVERAGE.CONNECT_FOUR_NEIGHBOURS_ORDINAL_NODUPES(_neighbours NUMERIC)
RETURNS INTEGER AS
$$ -- dollar quotes
DECLARE
  _curs REFCURSOR; -- cursor
  _rec RECORD;

BEGIN

-- create a copy of friction_points which will have records deleted sequentially
CREATE TEMPORARY TABLE TARGET_POINTS ON COMMIT DROP AS
        SELECT *
        FROM COVERAGE.FRICTION_B_POINTS;

-- create spatial indexes if they do not exist
DROP INDEX IF EXISTS gist_temp_target_geom;
CREATE INDEX IF NOT EXISTS gist_temp_target_geom ON TARGET_POINTS USING GIST (geometry);

DROP INDEX IF EXISTS gist_friction_geom;
CREATE INDEX IF NOT EXISTS gist_friction_geom ON COVERAGE.FRICTION_B_POINTS USING GIST (geometry);

DROP INDEX IF EXISTS gist_raster_geom;
CREATE INDEX IF NOT EXISTS gist_raster_geom ON COVERAGE.FRICTION_B_POINTS USING GIST (geometry);


-- create destination table
DROP TABLE IF EXISTS  COVERAGE.FRICTION_B_LINES_ORDINAL_NO_DUPES CASCADE;

CREATE TABLE COVERAGE.FRICTION_B_LINES_ORDINAL_NO_DUPES (
  Id BIGSERIAL,
  Source_Id BIGINT,
  Target_Id BIGINT,
  Source_fric VARCHAR(100),
  Target_fric VARCHAR(100),
  Distance VARCHAR(100),
  Cost VARCHAR(100),	
  Geom GEOMETRY(LINESTRING, 4326),
  CONSTRAINT FRICTION_B_LINES_ORDINAL_NO_DUPES_pk PRIMARY KEY(Id) 
);

DROP INDEX IF EXISTS gist_friction_geom;
CREATE INDEX IF NOT EXISTS gist_friction_geom ON COVERAGE.FRICTION_B_LINES_ORDINAL_NO_DUPES USING GIST (Geom);



-- notify
RAISE NOTICE 'CONNECT_FOUR_ORDINAL_NEIGHBOURS: begun.';

BEGIN

-- open cursor
OPEN _curs FOR SELECT id, geometry
                   FROM COVERAGE.FRICTION_B_RASTER_VERTICES
                   WHERE geometry IS NOT NULL
                   ;
LOOP

      -- get next record
      FETCH NEXT FROM _curs INTO _rec;
      EXIT WHEN NOT FOUND;

WITH SOURCE AS (
	SELECT *
	FROM COVERAGE.FRICTION_B_RASTER_VERTICES
	--WHERE id = 6895
	WHERE Id = _rec.Id
),
TARGETS AS (
	SELECT target_points.id, target_points.layer,
	target_points.geometry geometry,
	target_points.geometry <-> source.geometry AS dist
	FROM
	SOURCE source, TARGET_POINTS target_points
	ORDER BY
	dist
	LIMIT 4), 
NONDUPES AS (
	SELECT TARGETS.ID targets_id, SOURCE.ID source_id, targets.layer targets_friction,  TARGETS.dist, (targets.layer  * (TARGETS.dist/1000)) sum_friction, ST_Transform(ST_MakeLine(targets.geometry, source.geometry), 4326) geometry
	FROM TARGETS, SOURCE
	WHERE (TARGETS.ID <> SOURCE.ID) AND TARGETS.dist <1200)

INSERT INTO COVERAGE.FRICTION_B_LINES_ORDINAL_NO_DUPES (Source_Id, Target_Id, Target_fric, Distance, Cost,	Geom)
SELECT D.source_id, D.targets_id, D.targets_friction, D.dist,  D.sum_friction, D.geometry
FROM NONDUPES D
;

END LOOP;

-- Close the cursor
CLOSE _curs;
END;	
	
-- notify
RAISE NOTICE 'CONNECT_FOUR_ORDINAL_NEIGHBOURS: completed successfully.';
RETURN 0;

END;
$$
LANGUAGE plpgsql;