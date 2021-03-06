DROP TABLE IF EXISTS  COVERAGE.POT_AG_DEALERS CASCADE;

CREATE TABLE COVERAGE.POT_AG_DEALERS (
  Id BIGINT,  
  Pot_Ag_Dealer_Name VARCHAR(100),
  District_Name VARCHAR(100),
  Facility VARCHAR(100),
  X DOUBLE PRECISION,
  Y DOUBLE PRECISION,
  Geom GEOMETRY(POINT, 4236),
  CONSTRAINT pot_ag_dealers_pk PRIMARY KEY(Id) 
);