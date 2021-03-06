ALTER TABLE COVERAGE.CNFA_AG_DEALERS 
DROP COLUMN IF EXISTS Geom;

ALTER TABLE COVERAGE.CNFA_AG_DEALERS 
ADD COLUMN Geom GEOMETRY(POINT, 32736);

UPDATE COVERAGE.CNFA_AG_DEALERS 
    SET Geom = ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(X, Y), 4236), 32736)
        WHERE X IS NOT NULL AND X BETWEEN 30 AND 36 AND
          Y IS NOT NULL AND Y BETWEEN -80 AND 0;

-- EPSG:32736 WGS 84 / UTM zone 36S https://epsg.io/32736
