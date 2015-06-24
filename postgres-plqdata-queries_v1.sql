

DROP TABLE IF EXISTS descr;
CREATE TABLE descr (
    id                BIGINT       NOT NULL,
    effectiveTime     CHAR (8)     NOT NULL,
    active            INT          NOT NULL,
    moduleId          BIGINT       NOT NULL,
    conceptId         BIGINT          NOT NULL,
    languageCode      VARCHAR (2)  NOT NULL,
    typeId            BIGINT       NOT NULL,
    term              VARCHAR (255) NOT NULL,
    casSignificanceId BIGINT       NOT NULL,
    PRIMARY KEY (
        id,
        effectiveTime
    )
)
;
DROP TABLE IF EXISTS relats;
CREATE TABLE relats (
    id                   BIGINT       NOT NULL,
    effectivetime        CHAR (8)     NOT NULL,
    active               INT          NOT NULL,
    moduleid             BIGINT       NOT NULL,
    sourceid             BIGINT       NOT NULL,
    destinationid        BIGINT       NOT NULL,
    relationshipgroup    INT          NOT NULL,
    typeid               INT          NOT NULL,
    characteristictypeid BIGINT       NOT NULL,
    modifierid           BIGINT       NOT NULL,
    PRIMARY KEY (
        id,
        effectivetime
    ));


/* Preprocessing of files:
   - descr: Replace all " (double quotes) with %-&-% as double quotes can affect import
   - both: Delete header row
   - descr: Find terms which are "International Health Terminology, etc. etc. 
            and shorten to less than 255 chars
   - relats: Use stated relationships file rather than the other relationships file which
             is based on inferred relationships.
*/

DELETE FROM descr;
COPY descr
FROM '/Users/Shared/snomedct/descr-quotesnohead.txt';
UPDATE descr SET term = REPLACE(term, '%-&-%', '"');  -- restores the " 
                                                      -- 1,492,825 rows, 45sec

/* This takes the terms with matching typeIDs and matching active statuses and 
   finds the term with the latest effectivetime - "greatest-n-per-group". 
   Then it filters in only the active, fully-specified-name terms. This leaves 
   one unique, active, most recent term for each concept ID. */
   
DROP TABLE IF EXISTS descrshort;
SELECT d1.* 
INTO descrshort
FROM descr d1 LEFT OUTER JOIN descr d2 
ON (d1.conceptID = d2.conceptID 
    AND d1.effectivetime < d2.effectivetime 
    AND d1.active = d2.active 
    AND d1.typeID = d2.typeID)
WHERE d2.conceptID is NULL 
    AND d1.active = 1 AND d1.typeID = 900000000000003001; -- 406,310 rows, 3.7sec

DELETE FROM relats;
COPY relats
FROM '/Users/Shared/snomedct/relats-nohead.txt'; -- 787,514 rows, 8.4 sec

/* Create Indexes to make searches go faster */
CREATE INDEX source ON relats(sourceID,destinationID);
CREATE INDEX terms ON descrshort(conceptID); -- 4.3 sec

DROP TABLE IF EXISTS descr;

/* Preprocessing of files:
   - pldata: 
      - Open in xlsx file in Excel
      - Make 1 new columns next to PROBDESC and 2 new columns next to OVERVIEW.
      - Column next to Probdesc: =IF(ISBLANK(***),0,1) , where *** are the cells in PROBDESC
      - 1st Column next to Overview: =IF(ISBLANK(***),0,1) , where *** are the cells in OVERVIEW
      - 2nd Column next to Overview: =LEN(***) , where *** are the cells in OVERVIEW
      - Make sure all numerical columns are amply wide so that numbers are 
        not converted to scientific notation - note that SNOMED has 14-digit+ numbers
      - Save as Windows-formatted txt file (active sheet) /Users/Shared/pldata/pldata.txt
      - Open text file in Excel
      - Paste SNOMED column from Excel file into txt file SNOMED column (opening the text file seems to
        convert large numbers >10 digits to scientific notation)
      - Delete PROBDESC and OVERVIEW columns (New columns made in previous steps should retain values)
      - Reformat ADMITDATE, NOTEDDATE, ENTRYDATE, DELDATE, RESOLDATE as Custom: yyyy-mm-dd hh:mm:ss
      - Sort PHCC descending, replace all cells with 'Deleted 2014' with '0'
      - Re-sort by PTID, then PROBID, then NOTEDDATE, then ENTRYDATE
      - Re-save as Windows-formatted txt file
      - Open text file in TextEdit
      - Delete Header Row
      - Save again
   - disch:
      - Open xlsx file in Excel
      - Make sure all numerical columns are amply wide so that numbers are 
        not converted to scientific notation
      - Save as Windows-formatted text
      - Open text file in Excel
      - Reformat ADMITDATE, DISCHDATE, READMIT as Custom: yyyy-mm-dd hh:mm:ss
      - Sort by PT_AGE (descending)
      - Replace "2 WEEKS" with "0.5", and replace "3 WEEKS" with "0.75"
      - Sort by PTID then ADMITDATE
      - Save 
      - Open text file in TextEdit
      - Delete Header Row
      - Save again

*/

/* LOAD PLQ Data Files */
/* Problem List Data Table */
DROP TABLE IF EXISTS pldata;
CREATE TABLE pldata(
   probid     BIGINT NOT NULL,
   ptid       BIGINT,
   dischid    BIGINT,
   csn        BIGINT,
   mrn        BIGINT,
   admitdate  TIMESTAMP,
   snomed     BIGINT,
   pdxid      BIGINT,
   picd9      TEXT,
   picd10     TEXT,
   phcc       NUMERIC(4,3),
   probdesc   INT,
   picd9desc  TEXT,
   picd10desc TEXT,
   noteddate  TIMESTAMP,
   entrydate  TIMESTAMP,
   deldate    TIMESTAMP,
   resoldate  TIMESTAMP,
   dischprob  INT,
   ptobmatch  INT,
   overview   INT,
   overlen    INT,
   hospprob   INT,
   priority   VARCHAR(25),
   prov_id    BIGINT,
   provtype   VARCHAR(25),
   provdept   VARCHAR(100)
    )
;

DELETE FROM pldata;
COPY pldata
FROM '/Users/Shared/pldata/pldata.txt'
DELIMITER '	'
NULL ''; -- 1.26 sec, 71766 rows

/* Load disch data table */
DROP TABLE IF EXISTS disch;
CREATE TABLE disch (
   dischid     BIGINT,
   ptid        BIGINT,
   csn         BIGINT,
   mrn         BIGINT,
   pt_age      NUMERIC,
   pt_gender   INT,
   adm_ed      INT,
   adm_sched   INT,
   department  TEXT,
   service     TEXT,
   dischdate   TIMESTAMP WITHOUT TIME ZONE,
   admitdate   TIMESTAMP WITHOUT TIME ZONE,
   los         INT,
   losicu      INT,
   transfers   INT,
   specialties INT,
   pews_peak   INT,
   pews_mean   NUMERIC,
   readmit     TIMESTAMP WITHOUT TIME ZONE,
   opvisits    INT,
   ipvisits    INT,
   soi         TEXT,
   rom         TEXT,
   cc_time     INT
   );

DELETE FROM disch;
COPY disch
FROM '/Users/Shared/pldata/disch.txt'
DELIMITER '	'
NULL ''; -- 0.1 sec, 10394 rows

-- Fix a few pldata problems where the noteddate is missing. Replace with oldest entrydate.
ALTER TABLE pldata
ADD   dxlongit   TEXT;

UPDATE pldata SET dxlongit = ptid::text||pdxid::text||coalesce(noteddate, '1900-01-01')::text||coalesce(snomed,1010)::text

DROP TABLE IF EXISTS noteddatefix;
SELECT p1.dxlongit, p1.entrydate
INTO noteddatefix
FROM pldata p1 LEFT OUTER JOIN pldata p2
ON p1.dxlongit = p2.dxlongit AND p1.entrydate > p2.entrydate
WHERE p2.entrydate IS NULL AND p1.noteddate IS NULL;

UPDATE pldata AS p 
SET noteddate = n.entrydate
FROM noteddatefix AS n
WHERE p.dxlongit = n.dxlongit 
   AND p.dxlongit LIKE '%1900-01-01%';
   
SELECT *
FROM pldata WHERE noteddate IS NULL OR snomed IS NULL
ORDER BY dxlongit, entrydate;  
  -- solved the missing noteddate problem, but still have missing snomed in 179 rows

DROP TABLE IF EXISTS noteddatefix;

/* Exploratory Tables */
-- How many problems per patient?
SELECT ptid, COUNT(probid)
FROM pldata
GROUP BY ptid
ORDER BY COUNT(probid) DESC;

-- Pick a patient and look at characteristics of their problems
SELECT p.ptid, p.probid, p.pdxid, p.dischid, p.snomed, d.term, p.admitdate, p.noteddate, p.entrydate, p.resoldate, p.deldate
FROM pldata p, descrshort d
WHERE ptid = 104225590 AND p.snomed = d.conceptid --104257411
ORDER BY ptid, pdxid, noteddate, entrydate, probid, dischid;

-- Chart review on patient
SELECT DISTINCT mrn FROM pldata WHERE ptid = 104225590;

/* All Problems at each discharge for X patient */
DROP TABLE IF EXISTS dcprobstemp1;
SELECT d.ptid as ptid, d.dischid as dischid, p.pdxid as pdxid, p.probid as probid, p.snomed as conceptid, ds.term as term, 
       p.picd9 as picd9, p.phcc as phcc, d.csn as csn, d.mrn as mrn, d.admitdate as admitdate, d.dischdate as dischdate, 
       p.noteddate as noteddate, p.entrydate as entrydate, p.resoldate as resoldate, p.deldate as deldate, p.hospprob as hospprob,
       p.probdesc as probdesc, p.overview as overview, p.overlen as overlen, p.priority as priority, p.prov_id as prov_id, 
       p.provtype as provtype, p.provdept as provdept, date_trunc('day', d.dischdate)-p.entrydate as datediff, 
       CAST(p.ptid AS text)||CAST(d.dischid AS text)||CAST(p.pdxid AS text)||CAST(p.snomed AS text)||CAST(p.noteddate AS text) as dcprobid
INTO dcprobstemp1
FROM disch d, pldata p, descrshort ds
WHERE d.ptid = 104225590 AND p.ptid = 104225590 AND p.snomed = ds.conceptid
      AND date_trunc('day', p.noteddate) <= date_trunc('day', d.dischdate)
      AND (date_trunc('day', p.resoldate) > date_trunc('day', d.dischdate) 
             OR date_trunc('day', p.deldate) > date_trunc('day', d.dischdate) 
             OR (p.resoldate IS NULL AND p.deldate IS NULL))
      AND date_trunc('day', p.entrydate) <= date_trunc('day', d.dischdate)
      AND prov_id IS NOT NULL
ORDER BY d.ptid, d.admitdate, d.dischid, p.pdxid, p.probid;

SELECT * FROM dcprobstemp1;

-- choose the record with unique ptid, pdxid, conceptid, noteddate 
-- with the most recently updated prior to discharge
DROP TABLE IF EXISTS pldata2;
SELECT t3.*
INTO pldata2
FROM dcprobstemp1 t3
JOIN (
   SELECT t1.dcprobid, MAX(probid) as maxprobid
   FROM dcprobstemp1 t1
   JOIN (
       SELECT dcprobid, MIN(datediff) as mindatediff
       FROM dcprobstemp1
       GROUP BY dcprobid
        ) t2
   ON t1.dcprobid = t2.dcprobid
   AND t1.datediff = t2.mindatediff
   GROUP BY t1.dcprobid
     ) t4
ON t4.dcprobid = t3.dcprobid
AND t3.probid = t4.maxprobid;

SELECT * FROM pldata2;

/* All Problems at each discharge for All patients */
DROP TABLE IF EXISTS dcprobstemp1;
SELECT d.ptid as ptid, d.dischid as dischid, p.pdxid, p.probid as probid, 
       p.snomed as conceptid, ds.term as term, p.picd9 as picd9, p.phcc as phcc, 
       d.csn as csn, d.mrn as mrn, d.admitdate as admitdate, d.dischdate as dischdate, 
       p.noteddate as noteddate, p.entrydate as entrydate, p.resoldate as resoldate, 
       p.deldate as deldate, p.hospprob as hospprob, p.probdesc as probdesc, 
       p.overview as overview, p.overlen as overlen, p.priority as priority, 
       p.prov_id as prov_id, p.provtype as provtype, p.provdept as provdept, 
       date_trunc('day', d.dischdate)-p.entrydate as datediff,                 -- Calculate date diff btwn discharge & entry
       CAST(p.ptid AS text)||CAST(d.dischid AS text)||CAST(p.pdxid AS text)||
       CAST(p.snomed AS text)||CAST(p.noteddate AS text) as dcprobid           -- Create a unique ID for each problem over time
INTO dcprobstemp1
FROM disch d, pldata p, descrshort ds
WHERE d.ptid = p.ptid AND p.snomed = ds.conceptid
      AND date_trunc('day', p.noteddate) <= date_trunc('day', d.dischdate)     -- Problem must have originated prior to d/c
      AND (date_trunc('day', p.resoldate) > date_trunc('day', d.dischdate)     -- Problem must have not have been del 
             OR date_trunc('day', p.deldate) > date_trunc('day', d.dischdate)  -- or resol prior to d/c
             OR (p.resoldate IS NULL AND p.deldate IS NULL))
      AND date_trunc('day', p.entrydate) <= date_trunc('day', d.dischdate)     -- filter out negative date differences
      AND prov_id IS NOT NULL                                                  -- keep only human-entered problems
ORDER BY d.ptid, d.admitdate, d.dischid, p.pdxid, p.probid; --3.8 sec, 184350 rows

-- choose the problem list update most recently entered prior to each discharge
-- first choose by date difference between entry and discharge (negative differences 
-- filtered out beforehand). then in case of a tie, choose highest probid.

DROP TABLE IF EXISTS pldata2;
SELECT t3.*
INTO pldata2
FROM dcprobstemp1 t3
JOIN (
   SELECT t1.dcprobid, MAX(probid) as maxprobid
   FROM dcprobstemp1 t1
   JOIN (
       SELECT dcprobid, MIN(datediff) as mindatediff
       FROM dcprobstemp1
       GROUP BY dcprobid
        ) t2
   ON t1.dcprobid = t2.dcprobid
   AND t1.datediff = t2.mindatediff
   GROUP BY t1.dcprobid
     ) t4
ON t4.dcprobid = t3.dcprobid
AND t3.probid = t4.maxprobid; -- 2.1 sec. 58464 rows

--SELECT * FROM pldata2 LIMIT 1000;
DROP TABLE IF EXISTS dcprobstemp1;

/* Add columns to pldata2 */
ALTER TABLE pldata2
ADD   descendants    INT,
ADD   descendantslog REAL,
ADD   subsumed       INT,
ADD   duplic         INT,
ADD   finding        INT,
ADD   snomedtype     TEXT;

/* Determine duplicates */
-- Use Group by to find dischID/conceptID 
-- combos with multiple appearances 
DROP TABLE IF EXISTS duplicate;
SELECT dischID, conceptID
INTO duplicate
FROM pldata2
GROUP BY dischID, conceptID
HAVING COUNT(*) > 1;        --4662 rows, 0.1 sec

UPDATE pldata2 SET duplic = 0;  --1.1 sec
UPDATE pldata2 SET duplic = 1
FROM duplicate 
WHERE pldata2.dischID = duplicate.dischID 
   AND pldata2.conceptID = duplicate.conceptID; --11190 rows, 1.7 sec

/*SELECT p.dischID, p.conceptID, p.duplic, d.term
FROM pldata2 p JOIN descrshort d ON p.conceptID = d.conceptID
ORDER BY (p.dischID, p.conceptID) DESC
LIMIT 1000;*/

DROP TABLE duplicate;

/* Create a transitive closure table for easy look up of all descendants*/
-- Create a blank closure table:
DROP TABLE IF EXISTS closure;        
CREATE TABLE closure( 
   destinationID BIGINT NOT NULL, sourceID BIGINT NOT NULL, depth INT, 
   PRIMARY KEY(destinationID,sourceID));

-- Make a table with only distinct conceptIDs used in pldata
DROP TABLE IF EXISTS pldatatemp;
SELECT DISTINCT conceptID INTO TABLE pldatatemp FROM pldata2 ORDER BY conceptID;  -- 0.084 sec, 3701 rows

-- Find all descendants from conceptIDs above and insert into closure table
WITH RECURSIVE
  Descendants(sourceID,destinationID,depth) AS (   
   SELECT r3.sourceID AS sourceID, r3.destinationID AS destinationID, 1
   FROM relats AS r3, pldatatemp
   WHERE r3.destinationID = pldatatemp.conceptID 
   AND typeID = 116680003 AND active = 1 
   UNION
   SELECT r3.sourceID AS sourceID, r2.destinationID AS destinationID, r2.depth + 1
   FROM relats AS r3, Descendants AS r2
   WHERE r3.destinationID = r2.sourceID
   AND typeID = 116680003 AND active = 1
   AND r3.sourceID <> r2.destinationID
   AND r2.depth < 15) -- Greatest depth in SNOMEDCT from Clinical finding root is ~15
INSERT INTO closure 
SELECT DISTINCT d.destinationID, d.sourceID, MIN(d.depth) AS depth
FROM Descendants d
GROUP BY d.destinationID, d.sourceID;  -- 24.2 sec, 260059 rows

SELECT COUNT(*) FROM pldatatemp; -- 3701 problems
DROP TABLE IF EXISTS pldatatemp;

/* Find if each problem has an descendant on the same dischargeID*/
-- Put all problems that are subsumed as list into a new table
DROP TABLE IF EXISTS subsumed;
SELECT p.dischID, p.conceptID, c.sourceID as "Subsumed by", c.depth, 
       p.pdxid as pdxid1, p2.pdxid as pdxid2
INTO subsumed
FROM pldata2 p JOIN closure c ON c.destinationID = p.conceptID
               JOIN pldata2 p2 ON c.sourceID = p2.conceptID
WHERE p.dischID = p2.dischID 
          AND p.pdxid != p2.pdxid   -- filter out subsumed problems 2/2 dxid includes subsumed problems
ORDER BY p.dischID;       -- 3.1 sec, 8296 rows

-- Validate contents of subsumed
SELECT s.*, d1.term, d2.term FROM subsumed s, descrshort d1, descrshort d2
WHERE s.conceptID = d1.conceptID AND s."Subsumed by" = d2.conceptID
AND pdxid1 != pdxid2
ORDER BY (s.dischID, s.conceptID) DESC;
--4361 subsumed because of dxid -> snomed conversion
--8296 really subsumed

-- Update the problem list data table with info on whether problems are subsumed
UPDATE pldata2 SET subsumed = 0;  -- 1.0 sec
UPDATE pldata2 SET subsumed = 1
FROM subsumed
WHERE pldata2.dischID = subsumed.dischID and pldata2.conceptID = subsumed.conceptID; -- 0.4 sec, 5791 rows

SELECT p.dischID, p.conceptID, p.duplic, p.subsumed, d.term
FROM pldata2 p JOIN descrshort d ON p.conceptID = d.conceptID
ORDER BY (p.dischID, p.conceptID) DESC
LIMIT 1000;

-- Determine # of descendants per conceptID using closure table
UPDATE pldata2 SET descendants = NULL;    -- 0.8 sec, 58464 rows

DROP FUNCTION IF EXISTS DescLookup(bigint);
CREATE FUNCTION DescLookup(bigint) RETURNS bigint
    AS $$ SELECT COUNT(*) 
          FROM closure 
          WHERE destinationID = $1 $$
    LANGUAGE SQL;
UPDATE pldata2 SET descendants = coalesce(DescLookup(conceptID),0); --8.0 sec

-- Determine Log (1+ # of descendants per conceptID) using closure table
-- Log10 transformation to make descendants an easier number
DROP FUNCTION IF EXISTS LogDescLookup(bigint);
CREATE FUNCTION LogDescLookup(bigint) RETURNS double precision
    AS $$ SELECT Log(1+COUNT(*))
          FROM closure 
          WHERE destinationID = $1 $$
    LANGUAGE SQL;
UPDATE pldata2 SET descendantslog = coalesce(LogDescLookup(conceptID),1); --8.0 sec

SELECT p.dischID, p.probID, p.conceptID, p.duplic, p.subsumed, 
       p.descendants, p.descendantslog, d.term
FROM pldata2 p JOIN descrshort d ON p.conceptID = d.conceptID
ORDER BY (p.dischID, p.probID) DESC
LIMIT 1000;

DROP TABLE closure;
DROP TABLE subsumed;


/* Determine findings vs disorder */
UPDATE pldata2 SET finding = 0;
UPDATE pldata2 SET snomedtype = 'other';

UPDATE pldata2 SET finding = 1 
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(finding)%';

UPDATE pldata2 SET snomedtype = 'finding'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(finding)%';

UPDATE pldata2 SET snomedtype = 'morphology'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(morphologic abnormality)%';
 
UPDATE pldata2 SET snomedtype = 'procedure'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(procedure)%';
 
UPDATE pldata2 SET snomedtype = 'situation'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(situation)%';
 
UPDATE pldata2 SET snomedtype = 'organism'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(organism)%';
 
UPDATE pldata2 SET snomedtype = 'disorder'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(disorder)%';

UPDATE pldata2 SET snomedtype = 'person'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(person)%';

UPDATE pldata2 SET snomedtype = 'bodystruc'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(body structure)%';

UPDATE pldata2 SET snomedtype = 'regime'
FROM descrshort WHERE descrshort.conceptID = pldata2.conceptID 
AND descrshort.term LIKE '%(regime/therapy)%';
 
-- Show all calculated variables in table
SELECT p.dischID, p.probID, p.conceptID, p.duplic, p.subsumed, p.descendants, p.descendantslog, 
       p.finding, p.snomedtype, d.term
FROM pldata2 p JOIN descrshort d ON p.conceptID = d.conceptID
ORDER BY (p.dischID, p.probID) DESC
LIMIT 1000;

/* Create function to calculate median */
DROP FUNCTION IF EXISTS _final_median(anyarray);
CREATE FUNCTION _final_median(anyarray) RETURNS float8 AS $$ 
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM 
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)  
  ) q2;
$$ LANGUAGE sql IMMUTABLE;
 
CREATE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);

-- Show summary data for each discharge
DROP TABLE IF EXISTS plsumm;
CREATE TABLE plsumm(
  dischID          BIGINT,
  probcount        INT,
  snomedcount      INT,
  countresol       INT,
  percentresol     REAL,
  countdel         INT,
  percentdel       REAL,
  countdup         INT,
  percentdup       REAL,
  countsubs        INT,
  percentsubs      REAL,
  meandesc         REAL,
  mediandesc       REAL,
  meanlogdesc      REAL,
  countfinding     INT,
  percentfinding   REAL,
  counthospprob    INT,
  percenthospprob  REAL,
  countoverview    INT,
  percentoverview  REAL,
  avgoverlen       REAL,
  avgoverlenlog    REAL,
  countprobdesc    INT,
  percentprobdesc  REAL,
  countpriority    INT,
  percentpriority  REAL,
  avdatediff       REAL);

DELETE FROM plsumm;
INSERT INTO plsumm
SELECT dischID, COUNT(DISTINCT probid) as probcount, COUNT(conceptid) as snomedcount, 
       COUNT(resoldate) as countresol, COUNT(resoldate)/COUNT(conceptid)*100 as percentresol, 
       COUNT(deldate) as countdel, COUNT(deldate)/COUNT(conceptid)*100 as percentdel,
       SUM(duplic) as countdup, AVG(duplic)*100 as percentdup, 
       SUM(subsumed) as countsubs, AVG(subsumed)*100 as percentsubs, 
       AVG(descendants) as meandesc, median(descendants) as mediandesc,
       AVG(descendantslog) as meanlogdesc,
       SUM(finding) as countfinding, AVG(finding)*100 as percentfinding, 
       SUM(hospprob) as counthospprob, AVG(hospprob)*100 as percenthospprob,
       SUM(overview) as countoverview, AVG(overview)*100 as percentoverview, 
       AVG(overlen) as avgoverlen, AVG(LOG(1+overlen)) as avgoverlenlog,
       SUM(probdesc) as countprobdesc, AVG(probdesc)*100 as percentprobdesc, 
       COUNT(priority) as countpriority, COUNT(priority)/COUNT(conceptid)*100 as percentpriority, 
       AVG(EXTRACT(EPOCH FROM datediff)/86400) as avdatediff 
FROM pldata2
GROUP BY dischID
ORDER BY dischID;  -- 1.37 sec, 10042 rows

/* Put pl data into disch table */
DROP TABLE IF EXISTS disch2;
SELECT d.ptid, d.dischid, d.pt_age, d.pt_gender, d.adm_ed, d.adm_sched,
       d.department, d.service, d.admitdate, d.dischdate, d.los, d.losicu,
       d.transfers, d.specialties, d.pews_peak, d.pews_mean, 
       CAST(CASE WHEN d.readmit IS NULL                      -- Calculate readmit date for survival analysis
               THEN extract(EPOCH FROM (DATE '2015-06-06'-date_trunc('day', d.dischdate)))/86400 
               ELSE extract(EPOCH FROM (date_trunc('day', d.readmit)-date_trunc('day', d.dischdate)))/86400 
               END AS REAL) as readmitcensored, 
       CASE WHEN extract(EPOCH FROM (d.readmit-d.dischdate))/86400<31 
               THEN 1 ELSE 0 END as readmit30d,
       d.opvisits, d.ipvisits, d.cc_time, p.probcount, p.snomedcount, 
       p.countresol, p.percentresol, p.countdel, p.percentdel, p.countdup, p.percentdup, 
       p.countsubs, p.percentsubs, p.meandesc, p.mediandesc, p.meanlogdesc, p.countfinding,
       p.percentfinding, p.counthospprob, p.percenthospprob, p.countoverview, 
       p.percentoverview, p.avgoverlen, p.avgoverlenlog, p.countprobdesc, p.percentprobdesc, 
       p.countpriority, p.percentpriority, p.avdatediff
INTO disch2
FROM disch d LEFT OUTER JOIN plsumm p ON d.dischid = p.dischid
ORDER BY d.ptid, d.admitdate;  -- 0.12 sec

SELECT * FROM disch2 LIMIT 1000;

-- Show summary for all discharges
SELECT COUNT(DISTINCT probid) as probcount, COUNT(conceptid) as snomedcount, 
       COUNT(resoldate) as countresol, COUNT(resoldate)/COUNT(conceptid)*100 as percentresol, 
       COUNT(deldate) as countdel, COUNT(deldate)/COUNT(conceptid)*100 as percentdel,
       SUM(duplic) as countdup, AVG(duplic)*100 as percentdup, 
       SUM(subsumed) as countsubs, AVG(subsumed)*100 as percentsubs, 
       AVG(descendants) as meandesc, median(descendants) as mediandesc,
       SUM(finding) as countfinding, AVG(finding)*100 as percentfinding,  
       SUM(hospprob) as counthospprob, AVG(hospprob)*100 as percenthospprob,
       SUM(overview) as countoverview, AVG(overview)*100 as percentoverview, 
       AVG(overlen) as avgoverlen,
       SUM(probdesc) as countprobdesc, AVG(probdesc)*100 as percentprobdesc, 
       COUNT(priority) as countpriority, COUNT(priority)/COUNT(conceptid)*100 as percentpriority, 
       AVG(EXTRACT(EPOCH FROM datediff)/86400) as avdatediff  
FROM pldata2;

SELECT COUNT(*) FROM disch2 WHERE countpriority = 0; --9593 discharges with no priority list!

/* Make a separate table for all active problem list items during admission.
/* All Problems updated during each admission for All patients */
DROP TABLE IF EXISTS updated_during;
SELECT d.ptid, d.dischid, p.pdxid, p.probid, p.snomed as conceptid,
       ds.term, d.admitdate, d.dischdate, p.noteddate, p.entrydate,
       p.resoldate, p.deldate, p.hospprob, p.prov_id, p.provtype, p.provdept,
       EXTRACT(EPOCH FROM (date_trunc('day', d.dischdate)-p.entrydate)/86400) as datediff,
       EXTRACT(EPOCH FROM (p.entrydate-date_trunc('day', d.admitdate))/86400) as dayofentry,
       EXTRACT(EPOCH FROM (p.entrydate-date_trunc('day', d.admitdate))/(86400*greatest(d.los,1))) as dateposition,
       CAST(p.ptid AS text)||CAST(d.dischid AS text)||CAST(p.pdxid AS text)||
       CAST(p.snomed AS text)||CAST(p.noteddate AS text) as dcprobid,
       CASE WHEN date_trunc('day', p.entrydate)=date_trunc('day', p.deldate) THEN 1 ELSE 0 END as del,
       CASE WHEN date_trunc('day', p.entrydate)=date_trunc('day', p.resoldate) THEN 1 ELSE 0 END as resol,
       CASE WHEN date_trunc('day', p.entrydate)=date_trunc('day', p.noteddate) THEN 1 ELSE 0 END as noted
INTO updated_during
FROM disch d, pldata p, descrshort ds
WHERE d.ptid = p.ptid
  AND p.snomed = ds.conceptid
  AND date_trunc('day', p.noteddate) <= date_trunc('day', d.dischdate)
  AND (date_trunc('day', p.resoldate) >= date_trunc('day', d.admitdate) 
    OR date_trunc('day', p.deldate) >= date_trunc('day', d.admitdate)
        OR (p.deldate IS NULL AND p.resoldate IS NULL))
  AND date_trunc('day', p.entrydate) <= date_trunc('day', d.dischdate) 
  AND date_trunc('day', p.entrydate) >= date_trunc('day', d.admitdate)
  AND prov_id IS NOT NULL
ORDER BY d.ptid, d.admitdate, p.pdxid, p.probid; -- 3.4 sec, 48800 rows, 9891 dischid's

SELECT COUNT(DISTINCT dischid) FROM updated_during;

/* All Problems PRESENT during each admission for All patients */
DROP TABLE IF EXISTS present_during;
SELECT DISTINCT p.ptid, d.admitdate, d.dischdate, d.dischid, p.pdxid, 
                p.snomed as conceptid, ds.term, p.noteddate, p.resoldate, p.deldate,
                CAST(p.ptid AS text)||CAST(d.dischid AS text)||CAST(p.pdxid AS text)||
                CAST(p.snomed AS text)||CAST(p.noteddate AS text) as dcprobid,
                CAST(p.ptid AS text)||CAST(d.dischid AS text)||CAST(p.pdxid AS text)||
                CAST(p.noteddate AS text) as dcdxid,
                CAST(p.ptid AS text)||CAST(d.dischid AS text)||CAST(p.pdxid AS text) as dcbilldxid,
                p.picd9desc
INTO present_during
FROM disch d, pldata p, descrshort ds
WHERE d.ptid = p.ptid AND p.snomed = ds.conceptid
      AND date_trunc('day', p.noteddate) <= date_trunc('day', d.dischdate)     -- Problem must have originated prior to d/c
      AND (date_trunc('day', p.resoldate) >= date_trunc('day', d.admitdate)     -- Problem must have not have been del 
             OR date_trunc('day', p.deldate) >= date_trunc('day', d.admitdate)  -- or resol prior to admit
             OR (p.resoldate IS NULL AND p.deldate IS NULL))
ORDER BY p.ptid, d.admitdate, p.pdxid, p.snomed; -- 6.9 sec, 65305 rows, 10145 dischid's

SELECT COUNT(DISTINCT dischid) FROM present_during;

/* Find # Problems Present (Active) during each Admit-Discharge Period */
DROP TABLE IF EXISTS pr_present;
CREATE TABLE pr_present(
  dischid           BIGINT,
  dx_active         REAL,       -- active dxid's
  pr_active         REAL);      -- active snomed id's
DELETE FROM pr_present;
INSERT INTO pr_present
SELECT dischid, COUNT(DISTINCT dcdxid) as dx_active, COUNT(DISTINCT dcprobid) as pr_active
FROM present_during
GROUP BY dischid
ORDER BY dischid;                   -- 10145 rows
SELECT * FROM pr_present LIMIT 100; 

-- Total active DXIDs (49284) and active SNOMED ID's (63276)
SELECT COUNT(DISTINCT dcdxid) as dx_active, COUNT(DISTINCT dcprobid) as pr_active
FROM present_during;

/* Find # Problems Updated during each Admit-Discharge Period */
/* Find # Updates for all Problems during each Admit-Discharge Period */

DROP TABLE IF EXISTS pr_updated;
CREATE TABLE pr_updated(
  dischid           BIGINT,
  pr_updated        INT,     -- Number of updated problems
  tot_updates       INT,     -- Number of total updates
  del_updates       INT,     -- Number of problem deletion updates
  resol_updates     INT,     -- Number of problem resolution updates
  noted_updates     INT,     -- Number of problem (first) entry updates
  perc_resol_upd    REAL,    -- Percent of updates which are resolutions
  perc_del_upd      REAL,    -- Percent of updates which are deletions
  perc_noted_upd    REAL,    -- Percent of updates which are new entries
  avgdayofentry     REAL,    -- Average day of admission problems were updated
  avgdateposition   REAL     -- Average relative time admission problems were updated
  );                            -- where 0 = admission date and 1 = discharge date
DELETE FROM pr_updated;
INSERT INTO pr_updated
SELECT dischid, COUNT(DISTINCT dcprobid) as pr_updated, COUNT(*) as tot_updates, 
       SUM(del) as del_updates, SUM(resol) as resol_updates, SUM(noted) as noted_updates,
       AVG(resol)*100 as perc_resol_upd, AVG(del)*100 as perc_del_upd, 
       AVG(noted)*100 as perc_noted_upd,
       AVG(dayofentry) as avgdayofentry, AVG(dateposition) as avgdateposition
FROM updated_during
GROUP BY dischid
ORDER BY tot_updates DESC;         -- 9891 rows, 0.3 sec
SELECT * FROM pr_updated LIMIT 100;

-- Total updated problems, problem updates, deletions, resolutions, noteds, 
  -- and average problem update day of admission and relative time problems were updated
SELECT COUNT(DISTINCT dcprobid) as pr_updated, COUNT(*) as tot_updates, 
       SUM(del) as del_updates, SUM(resol) as resol_updates, SUM(noted) as noted_updates,
       AVG(resol)*100::real as perc_resol_upd, AVG(del)*100::real as perc_del_upd, 
       AVG(noted)*100::real as perc_noted_upd,
       AVG(dayofentry) as avgdayofentry, AVG(dateposition) as avgdateposition
FROM updated_during;

DROP TABLE IF EXISTS pusumm;
CREATE TABLE pusumm(
  dischid            BIGINT,
  dx_active          INT,
  pr_active          INT,
  pr_updated         INT,
  pr_notupdated      INT,  -- problem not updated during admit
  perc_updated       real, -- percent of problems updated during admit
  tot_updates        INT,
  upd_perupdprob     real, -- average # updates per updated problems, per admit
  upd_peractprob     real, -- average # updates per active problem, per admit
  del_updates        INT,
  resol_updates      INT,
  noted_updates      INT,
  perc_resol_upd    REAL,    -- Percent of updates which are resolutions
  perc_del_upd      REAL,    -- Percent of updates which are deletions
  perc_noted_upd    REAL,    -- Percent of updates which are new entries
  perc_resol_upd1    REAL,    -- Percent of updates which are resolutions
  perc_del_upd1      REAL,    -- Percent of updates which are deletions
  perc_noted_upd1    REAL,    -- Percent of updates which are new entries
  avgdayofentry      real,
  avgdateposition    real
  );

/* Join the above two tables together */
DELETE FROM pusumm;
INSERT INTO pusumm
SELECT p1.dischid, p1.dx_active, p1.pr_active, COALESCE(p2.pr_updated, 0),
       COALESCE(p1.pr_active-p2.pr_updated, p1.pr_active) as pr_notupdated, 
       COALESCE(p2.pr_updated/p1.pr_active*100, 0) as perc_updated, 
       COALESCE(p2.tot_updates, 0),
       COALESCE(p2.tot_updates/p2.pr_updated, 0) as upd_perupdprob,
       COALESCE(p2.tot_updates/p1.pr_active, 0) as upd_peractprob,
       COALESCE(p2.del_updates, 0), COALESCE(p2.resol_updates, 0), 
       COALESCE(p2.noted_updates, 0), perc_resol_upd, perc_del_upd, perc_noted_upd,
       COALESCE(p2.perc_resol_upd, 0) as perc_resol_upd1,
       COALESCE(p2.perc_del_upd, 0) as perc_del_upd1,
       COALESCE(p2.perc_noted_upd, 0) as perc_noted_upd1,
       p2.avgdayofentry, p2.avgdateposition
FROM pr_present p1 LEFT OUTER JOIN pr_updated p2
ON p1.dischid = p2.dischid
ORDER BY p1.dischid DESC; -- 0.1sec, 10145 rows

SELECT * FROM pusumm LIMIT 1000;

/* Merge data into disch table */

DROP TABLE IF EXISTS disch3;
SELECT d.*, p.dx_active, p.pr_active, p.pr_updated, p.pr_notupdated, p.perc_updated, p.tot_updates,
       p.upd_perupdprob, p.upd_peractprob, p.del_updates, p.resol_updates, p.noted_updates,
       p.perc_resol_upd, p.perc_del_upd, p.perc_noted_upd, 
       p.perc_resol_upd1, p.perc_del_upd1, p.perc_noted_upd1,
       p.avgdayofentry, p.avgdateposition
INTO disch3
FROM disch2 d LEFT OUTER JOIN pusumm p 
ON d.dischid = p.dischid
ORDER BY d.ptid, d.admitdate;  -- 10394 rows, 0.12 sec
SELECT * FROM disch3 LIMIT 200;

/* Preprocessing of billdata.xlsx file
 - Open xlsx file
 - Sort by PHCC descending and delete all "Delete 2014"
 - Make sure all numerical columns are amply wide so that numbers are 
   not converted to scientific notation
 - Save as Windows-formatted text
 - Open text file in Excel
 - Sort by PTID then DISCHID then BEPICDXID
 - Save 
 - Open text file in TextEdit
 - Delete Header Row
 - Save again

/* Load the Bill data Table */
DROP TABLE IF EXISTS billdata;
CREATE TABLE billdata(
  billingid          BIGINT,
  ptid               BIGINT,
  dischid            BIGINT,
  bepicdxid          BIGINT,
  bicd9              TEXT,
  bicd9desc          TEXT,
  bicd10             TEXT,
  bicd10desc         TEXT,
  bhcc               NUMERIC(4,3),
  btopmatch          INT
  );

DELETE FROM billdata;
COPY billdata
FROM '/Users/Shared/pldata/billdata.txt'
DELIMITER '	'
NULL '';  -- 63464 rows, 0.5 sec

ALTER TABLE billdata
ADD    dcxbillid   TEXT;
UPDATE billdata SET dcxbillid = CAST(ptid AS text)||CAST(dischid AS text)||CAST(bepicdxid AS text);

-- Create a match table of problem list dxid vs. billing data dxid
DROP TABLE IF EXISTS pbmatchdx;
SELECT DISTINCT coalesce(p.ptid, b.ptid) as ptid, coalesce(p.dischid, b.dischid) as dischid, 
     p.pdxid, b.bepicdxid, coalesce(b.bicd9desc, p.picd9desc) as icd9desc, p.dcbilldxid, b.dcxbillid
INTO pbmatchdx
FROM present_during p FULL OUTER JOIN billdata b
ON p.dcbilldxid = b.dcxbillid
ORDER BY coalesce(p.ptid, b.ptid), coalesce(p.dischid, b.dischid), p.pdxid, b.bepicdxid;
SELECT COUNT(DISTINCT dcbilldxid) FROM pbmatchdx;   -- # of distinct problem list dxids for all discharges=48707
SELECT COUNT(DISTINCT dcxbillid) FROM pbmatchdx;    -- # of distinct bill data dxid for all discharges=62251
SELECT COUNT(DISTINCT dcxbillid) FROM pbmatchdx WHERE dcxbillid = dcbilldxid; -- Only 1969 matches
SELECT COUNT(DISTINCT dcxbillid) FROM pbmatchdx WHERE dcbilldxid IS NULL; -- 60282 bill dxids unmatched
SELECT COUNT(DISTINCT dcbilldxid) FROM pbmatchdx WHERE dcxbillid IS NULL; -- 46738 problem list dxids unmatched

-- Create a match table of problem list snomed and dxid vs. billing data dxid
DROP TABLE IF EXISTS pbmatchpr;
SELECT DISTINCT coalesce(p.ptid, b.ptid) as ptid, coalesce(p.dischid, b.dischid) as dischid, 
    p.pdxid, p.conceptid, b.bepicdxid, coalesce(b.bicd9desc, p.picd9desc) as icd9desc, 
    p.dcbilldxid, b.dcxbillid, p.dcprobid
INTO pbmatchpr
FROM present_during p FULL OUTER JOIN billdata b
ON p.dcbilldxid = b.dcxbillid
ORDER BY coalesce(p.ptid, b.ptid), coalesce(p.dischid, b.dischid), p.pdxid, b.bepicdxid;
SELECT * FROM pbmatchpr LIMIT 100;
SELECT COUNT(DISTINCT dcprobid) FROM pbmatchpr;  -- 64841 total problem list SNOMED IDs 
SELECT COUNT(DISTINCT dcxbillid) FROM pbmatchpr; -- 62251 total bill data dxids
SELECT COUNT(DISTINCT dcprobid) FROM pbmatchpr WHERE dcxbillid = dcbilldxid; -- 3814 SNOMED IDs match
SELECT COUNT(DISTINCT dcxbillid) FROM pbmatchpr WHERE dcprobid IS NULL; -- 60282 bill data dxids unmatched
SELECT COUNT(DISTINCT dcprobid) FROM pbmatchpr WHERE dcxbillid IS NULL; -- 61027 SNOMED IDs unmatched

-- Calculate matching statistics for dxid matching
DROP TABLE IF EXISTS pbmatchsummdx;
SELECT ptid, dischid, 
                CAST(SUM((pdxid IS NOT NULL)::int) AS REAL) as PLDxIDs, 
                CAST(SUM((bepicdxid IS NOT NULL)::int) AS REAL) as BillDxIDs,
                CAST(SUM((pdxid IS NOT NULL AND bepicdxid IS NOT NULL)::int) AS REAL) as DxIDMatch,
                CAST(SUM((pdxid IS NOT NULL AND bepicdxid IS NULL)::int) AS REAL) as UnmatchedPLDxID,
                CAST(SUM((pdxid IS NULL AND bepicdxid IS NOT NULL)::int) AS REAL) as UnmatchedBillDxID
INTO pbmatchsummdx
FROM pbmatchdx
GROUP BY ptid, dischid
ORDER BY ptid, dischid; -- 10385 rows, 0.4 sec

-- Calculate matching statistics for snomed ID matching
DROP TABLE IF EXISTS pbmatchsummpr;
SELECT ptid, dischid, 
                CAST(SUM((conceptid IS NOT NULL)::int) AS REAL) as PLConceptIDs, 
                CAST(SUM((conceptid IS NOT NULL AND bepicdxid IS NOT NULL)::int) AS REAL) as ConceptIDMatch,
                CAST(SUM((conceptid IS NOT NULL AND bepicdxid IS NULL)::int) AS REAL) as UnmatchedConceptID
INTO pbmatchsummpr
FROM pbmatchpr
GROUP BY ptid, dischid
ORDER BY ptid, dischid;

/* Merge Billing-Problem List Matching Data into Discharge Table */
DROP TABLE IF EXISTS disch4;
SELECT d.*, p2.pldxids, p2.plconceptids, p2.billdxids, p2.dxidmatch, p2.conceptidmatch, 
            p2.unmatchedpldxid, p2.unmatchedbilldxid, p2.unmatchedconceptid
INTO disch4
FROM disch3 d LEFT OUTER JOIN (
     SELECT p1.*, p.plconceptids, p.conceptidmatch, p.unmatchedconceptid
     FROM pbmatchsummpr p FULL JOIN pbmatchsummdx p1 ON p.dischid = p1.dischid
     ) p2
ON d.dischid = p2.dischid
ORDER BY d.ptid, d.admitdate;  

SELECT * FROM disch4 LIMIT 1000;

SELECT COUNT(DISTINCT dischid) FROM disch; -- 10394 
SELECT COUNT(DISTINCT dischid) FROM plsumm; -- 10042 (All discharges with problems at discharge)
SELECT COUNT(DISTINCT dischid) FROM disch2; -- 10394
SELECT COUNT(DISTINCT dischid) FROM pr_updated; -- 9891 (All discharges with updated problems during admission)
SELECT COUNT(DISTINCT dischid) FROM pr_present; -- 10145 (All discharges with problems during admission)
SELECT COUNT(DISTINCT dischid) FROM pusumm; -- 10145 (All discharges with problems during admission)
SELECT COUNT(DISTINCT dischid) FROM disch3; -- 10394
SELECT COUNT(DISTINCT dischid) FROM billdata; --10358 (All discharges with either problem list OR bill data)
SELECT COUNT(DISTINCT dischid) FROM disch4; -- 10394

-- Clean up some tables
DROP TABLE IF EXISTS disch2;
DROP TABLE IF EXISTS disch3;
DROP TABLE IF EXISTS plsumm;
DROP TABLE IF EXISTS pr_present;
DROP TABLE IF EXISTS pr_updated;
DROP TABLE IF EXISTS pusumm;
DROP TABLE IF EXISTS pbmatchdx;
DROP TABLE IF EXISTS pbmatchpr;
DROP TABLE IF EXISTS pbmatchsummdx;
DROP TABLE IF EXISTS pbmatchsummpr;

/* Tables useful for analysis in R:
- disch4: For analysis of discharge data, including summary stats on problems and 
          bill data within each discharge
- updated_during: For analysis of all problem updates which happened during an 
          admission
- present_during: For analysis of all problems active during admissions
- pldata: still includes ALL problem updates, regardless of whether it was during 
          an admission

How many updates happen outside of admission? */

SELECT COUNT(*) FROM pldata; -- 71766 rows
SELECT COUNT(*) FROM updated_during; -- 48800 rows
SELECT DISTINCT p.ptid, p.noteddate, p.pdxid, p.entrydate, p.probid, p.snomed, 
                p.resoldate, p.deldate, p.picd9desc, p.provtype, p.provdept
FROM pldata p LEFT OUTER JOIN updated_during u
ON p.probid = u.probid
WHERE u.probid IS NULL
ORDER BY p.ptid, p.noteddate, p.pdxid, p.entrydate, p.snomed; -- 23336 rows, 1.4 sec

-- Further Clean up
DROP TABLE IF EXISTS pldata4;
DROP TABLE IF EXISTS disch5;
DROP TABLE IF EXISTS disch4;
DROP TABLE IF EXISTS pldata2;
DROP TABLE IF EXISTS present_during;
DROP TABLE IF EXISTS updated_during;


DROP TABLE IF EXISTS disch;
DROP TABLE IF EXISTS pldata;
DROP TABLE IF EXISTS billdata;
DROP TABLE IF EXISTS descrshort;
DROP TABLE IF EXISTS relats;
DROP TABLE IF EXISTS descr;