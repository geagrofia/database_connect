-- example: select COVERAGE.CONNECT_EIGHT_NEIGHBOURS_NODUPES(8)

CREATE OR REPLACE FUNCTION COVERAGE.CONNECT_EIGHT_NEIGHBOURS_NODUPES(_neighbours NUMERIC)
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

DROP INDEX IF EXISTS gist_temp_target_geom;
CREATE INDEX IF NOT EXISTS gist_temp_target_geom ON TARGET_POINTS USING GIST (geometry);

DROP INDEX IF EXISTS gist_friction_geom;
CREATE INDEX IF NOT EXISTS gist_friction_geom ON COVERAGE.FRICTION_B_POINTS USING GIST (geometry);

DROP TABLE IF EXISTS  COVERAGE.FRICTION_B_LINES_NO_DUPES CASCADE;

CREATE TABLE COVERAGE.FRICTION_B_LINES_NO_DUPES (
  Id BIGSERIAL,
  Source_Id BIGINT,
  Target_Id BIGINT,
  Source_fric VARCHAR(100),
  Target_fric VARCHAR(100),
  Distance VARCHAR(100),
  Combined_fric VARCHAR(100),	
  Geom GEOMETRY(LINESTRING, 4326),
  CONSTRAINT FRICTION_B_LINES_NO_DUPES_pk PRIMARY KEY(Id) 
);

DROP INDEX IF EXISTS gist_friction_geom;
CREATE INDEX IF NOT EXISTS gist_friction_geom ON COVERAGE.FRICTION_B_LINES_NO_DUPES USING GIST (Geom);



-- notify
RAISE NOTICE 'CONNECT_EIGHT_NEIGHBOURS: begun.';

BEGIN

-- open cursor
OPEN _curs FOR SELECT id, layer, geometry
                   FROM COVERAGE.FRICTION_B_POINTS
                   WHERE geometry IS NOT NULL
                   ;
LOOP

      -- get next record
      FETCH NEXT FROM _curs INTO _rec;
      EXIT WHEN NOT FOUND;

-- remove source point from target points temp table

			DELETE FROM TARGET_POINTS
			WHERE Id = _rec.Id;
      EXIT WHEN (SELECT COUNT(*) FROM TARGET_POINTS) = 0 ;

WITH SOURCE AS (
	SELECT *
	FROM COVERAGE.FRICTION_B_POINTS
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
	LIMIT 9), 
NONDUPES AS (
	SELECT TARGETS.ID targets_id, SOURCE.ID source_id, targets.layer targets_friction,  SOURCE.layer source_friction, TARGETS.dist, (((targets.layer + SOURCE.layer) / 2) * (TARGETS.dist/1000)) sum_friction, ST_Transform(ST_MakeLine(targets.geometry, source.geometry), 4326) geometry
	FROM TARGETS, SOURCE
	WHERE (TARGETS.ID <> SOURCE.ID) AND TARGETS.dist <1500)

INSERT INTO COVERAGE.FRICTION_B_LINES_NO_DUPES (Source_Id, Target_Id, Source_fric, Target_fric, Distance, Combined_fric,	Geom)
SELECT D.source_id, D.targets_id, D.source_friction, D.targets_friction, D.dist,  D.sum_friction, D.geometry
FROM NONDUPES D
;

END LOOP;

-- Close the cursor
CLOSE _curs;
END;	
	
-- notify
RAISE NOTICE 'CONNECT_EIGHT_NEIGHBOURS: completed successfully.';
RETURN 0;

END;
$$
LANGUAGE plpgsql;