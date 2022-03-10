-- example: select COVERAGE.REMOVE_GEOM_DUPES()
-- function remove_geom_dupes

CREATE OR REPLACE FUNCTION COVERAGE.REMOVE_GEOM_DUPES()
RETURNS INTEGER AS
$$
BEGIN

  -- notify
  RAISE NOTICE 'FUNCTION REMOVE_GEOM_DUPES: begun.';

  BEGIN
  
  -- add source-target and target-source fields to friction lines

ALTER TABLE COVERAGE.FRICTION_B_LINES
ADD COLUMN geom_dupe BOOLEAN DEFAULT FALSE;

-- duplications detected

WITH NONDUPES AS (
  SELECT L.Id, L.Source_Id, L.Target_Id, CONCAT(L.Source_Id, L.Target_Id) s_t, CONCAT(L.Target_Id, L.Source_Id) t_s
  FROM COVERAGE.FRICTION_B_LINES L),
DUPES AS (
  SELECT N1.Id
  FROM NONDUPES N1, NONDUPES N2
  WHERE N1.s_t = N2.t_s)

-- update error field to true if duplications detected

UPDATE COVERAGE.FRICTION_B_LINES
SET L.geom_dupe = TRUE
FROM COVERAGE.FRICTION_B_LINES L, DUPES D
WHERE (L.Id = D.Id) ; 

  
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FUNCTION REMOVE_GEOM_DUPES: error occurred';
    RETURN -1;
  END;

  -- notify
  RAISE NOTICE 'FUNCTION REMOVE_GEOM_DUPES: completed successfully.';
  RETURN 0;

END;
$$
LANGUAGE plpgsql;