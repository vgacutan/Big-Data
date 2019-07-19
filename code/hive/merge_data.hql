-- ***************************************************************************
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv

-- IMPORTANT NOTES:
-- You need to put events.csv and mortality.csv under hdfs directory 
-- '/input/events/events.csv' and '/input/mortality/mortality.csv'
-- 
-- To do this, run the following commands for events.csv, 
-- 1. sudo su - hdfs
-- 2. hdfs dfs -mkdir -p /input/events
-- 3. hdfs dfs -chown -R vagrant /input
-- 4. exit 
-- 5. hdfs dfs -put /path-to-events.csv /input/events/
-- Follow the same steps 1 - 5 for mortality.csv, except that the path should be 
-- '/input/mortality'
-- ***************************************************************************
-- create events table 
DROP TABLE IF EXISTS biospecimens;
CREATE EXTERNAL TABLE biospecimens (
PATNO STRING, 
GENDER STRING,  
DIAGNOSIS STRING, 
CLINICAL_EVENT STRING,  
TYPE STRING,
TESTNAME STRING,
TESTVALUE STRING,
UNITS FLOAT, 
RUNDATE STRING,
PROJECTID INT,
PI_NAME STRING,
PI_INSTITUTION STRING,
update_stamp STRING)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../data/biospecimens.csv'
OVERWRITE INTO TABLE biospecimens;

DROP VIEW IF EXISTS biospec;
CREATE VIEW biospec 
AS
SELECT DISTINCT PATNO,DIAGNOSIS
FROM biospecimens
ORDER BY PATNO;

-- *****
DROP TABLE IF EXISTS FamilyHistoryPD;
CREATE EXTERNAL TABLE FamilyHistoryPD (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING,
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,  
BIOMOM INT,  
BIOMOMPD INT,  
BIODAD INT,  
BIODADPD INT,  
FULSIB INT,  
FULSIBPD INT,  
HAFSIB INT,  
HAFSIBPD INT,  
MAGPAR INT,  
MAGPARPD INT,  
PAGPAR INT,  
PAGPARPD INT,  
MATAU INT,  
MATAUPD INT, 
PATAU INT, 
PATAUPD INT, 
KIDSNUM INT, 
KIDSPD INT,  
ORIG_ENTRY STRING,  
LAST_UPDATE STRING, 
QUERY STRING, 
SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../data/Family_History__PD_.csv'
OVERWRITE INTO TABLE FamilyHistoryPD;


DROP VIEW IF EXISTS famHistPD;
CREATE VIEW famHistPD 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","BIOMOM","TESTVALUE", BIOMOM),
         named_struct("TESTNAME","BIOMOMPD","TESTVALUE", BIOMOMPD),
         named_struct("TESTNAME","BIODAD","TESTVALUE", BIODAD),
         named_struct("TESTNAME","BIODADPD","TESTVALUE", BIODADPD),
         named_struct("TESTNAME","FULSIB","TESTVALUE", FULSIB),
         named_struct("TESTNAME","FULSIBPD","TESTVALUE", FULSIBPD),
         named_struct("TESTNAME","HAFSIB","TESTVALUE", HAFSIB),
         named_struct("TESTNAME","HAFSIBPD","TESTVALUE", HAFSIBPD),
         named_struct("TESTNAME","MAGPAR","TESTVALUE", MAGPAR),
         named_struct("TESTNAME","MAGPARPD","TESTVALUE", MAGPARPD),
         named_struct("TESTNAME","PAGPAR","TESTVALUE", PAGPAR),
         named_struct("TESTNAME","PAGPARPD","TESTVALUE", PAGPARPD),
         named_struct("TESTNAME","MATAU","TESTVALUE", MATAU),
         named_struct("TESTNAME","MATAUPD","TESTVALUE", MATAUPD),
         named_struct("TESTNAME","PATAU","TESTVALUE", PATAU),
         named_struct("TESTNAME","PATAUPD","TESTVALUE", PATAUPD),
         named_struct("TESTNAME","KIDSNUM","TESTVALUE", KIDSNUM),
         named_struct("TESTNAME","KIDSPD","TESTVALUE", KIDSPD)
    ) AS x
  FROM FamilyHistoryPD
) t1 LATERAL VIEW explode(x) t2 as y;


-- *****
DROP TABLE IF EXISTS GeneralNeurologicalExam;
CREATE EXTERNAL TABLE GeneralNeurologicalExam (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,  
MSRARSP INT,
MSRACM  STRING,
MSLARSP INT,
MSLACM  STRING,
MSRLRSP INT, 
MSRLCM  STRING,
MSLLRSP INT,
MSLLCM  STRING,
COFNRRSP INT,
COFNRCM STRING,
COFNLRSP INT,
COFNLCM STRING,
COHSRRSP INT,
COHSRCM STRING,
COHSLRSP INT,
COHSLCM STRING, 
SENRARSP INT,  
SENRACM STRING, 
SENLARSP INT, 
SENLACM STRING,
SENRLRSP INT,
SENRLCM STRING,
SENLLRSP INT,
SENLLCM STRING,
RFLRARSP INT,  
RFLRACM STRING, 
RFLLARSP INT,  
RFLLACM STRING, 
RFLRLRSP INT, 
RFLRLCM STRING,
RFLLLRSP INT, 
RFLLLCM STRING,
PLRRRSP INT,
PLRRCM STRING,  
PLRLRSP INT,
PLRLCM STRING, 
ORIG_ENTRY STRING,
LAST_UPDATE STRING, 
QUERY STRING, 
SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../data/General_Neurological_Exam.csv'
OVERWRITE INTO TABLE GeneralNeurologicalExam;


DROP VIEW IF EXISTS GenNeurologicalExam;
CREATE VIEW GenNeurologicalExam 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","MSRARSP","TESTVALUE", MSRARSP),
         named_struct("TESTNAME","MSLARSP","TESTVALUE", MSLARSP),
         named_struct("TESTNAME","MSRLRSP","TESTVALUE", MSRLRSP),
         named_struct("TESTNAME","MSLLRSP","TESTVALUE", MSLLRSP),
         named_struct("TESTNAME","COFNRRSP","TESTVALUE", COFNRRSP),
         named_struct("TESTNAME","COFNLRSP","TESTVALUE", COFNLRSP),
         named_struct("TESTNAME","COHSRRSP","TESTVALUE", COHSRRSP),
         named_struct("TESTNAME","SENRARSP","TESTVALUE", SENRARSP),
         named_struct("TESTNAME","SENLARSP","TESTVALUE", SENLARSP),
         named_struct("TESTNAME","SENRLRSP","TESTVALUE", SENRLRSP),
         named_struct("TESTNAME","SENLLRSP","TESTVALUE", SENLLRSP),
         named_struct("TESTNAME","RFLRARSP","TESTVALUE", RFLRARSP),
         named_struct("TESTNAME","RFLLARSP","TESTVALUE", RFLLARSP),
         named_struct("TESTNAME","RFLRLRSP","TESTVALUE", RFLRLRSP),
         named_struct("TESTNAME","RFLLLRSP","TESTVALUE", RFLLLRSP),
         named_struct("TESTNAME","PLRRRSP","TESTVALUE", PLRRRSP),
         named_struct("TESTNAME","PLRLRSP","TESTVALUE", PLRLRSP)
      ) AS x
  FROM GeneralNeurologicalExam
) t1 LATERAL VIEW explode(x) t2 as y;


-- *****
DROP TABLE IF EXISTS MDS_UPDRS_Part_III_Post_Dose_;
CREATE EXTERNAL TABLE MDS_UPDRS_Part_III_Post_Dose_ (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,
CMEDTM STRING,  
EXAMTM STRING,  
NP3SPCH INT, 
NP3FACXP INT,  
NP3RIGN INT, NP3RIGRU INT,  NP3RIGLU INT,  PN3RIGRL INT,  NP3RIGLL INT,  NP3FTAPR INT,  NP3FTAPL INT,  NP3HMOVR INT, NP3HMOVL INT,  NP3PRSPR INT, NP3PRSPL INT, NP3TTAPR INT, NP3TTAPL INT, NP3LGAGR INT,  NP3LGAGL INT,  NP3RISNG INT,  NP3GAIT INT, NP3FRZGT INT,  NP3PSTBL INT,  NP3POSTR INT, NP3BRADY INT,  NP3PTRMR INT,  NP3PTRML INT,  NP3KTRMR INT, NP3KTRML INT,  NP3RTARU INT, NP3RTALU INT,  NP3RTARL INT,  NP3RTALL INT,  NP3RTALJ INT,  NP3RTCON INT,  DYSKPRES INT,  DYSKIRAT INT,  NHY INT, ORIG_ENTRY STRING, LAST_UPDATE  STRING, QUERY STRING, SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../data/MDS_UPDRS_Part_III__Post_Dose_.csv'
OVERWRITE INTO TABLE MDS_UPDRS_Part_III_Post_Dose_;

DROP VIEW IF EXISTS MDSUPDRSPartIIIPostDose;
CREATE VIEW MDSUPDRSPartIIIPostDose 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","NP3SPCH","TESTVALUE", NP3SPCH),
         named_struct("TESTNAME","NP3FACXP","TESTVALUE", NP3FACXP),
         named_struct("TESTNAME","NP3RIGN","TESTVALUE", NP3RIGN),
         named_struct("TESTNAME","NP3RIGRU","TESTVALUE", NP3RIGRU),
         named_struct("TESTNAME","NP3RIGLU","TESTVALUE", NP3RIGLU),
         named_struct("TESTNAME","PN3RIGRL","TESTVALUE", PN3RIGRL),
         named_struct("TESTNAME","NP3RIGLL","TESTVALUE", NP3RIGLL),
         named_struct("TESTNAME","NP3FTAPR","TESTVALUE", NP3FTAPR),
         named_struct("TESTNAME","NP3FTAPL","TESTVALUE", NP3FTAPL),
         named_struct("TESTNAME","NP3HMOVR","TESTVALUE", NP3HMOVR),
         named_struct("TESTNAME","NP3HMOVL","TESTVALUE", NP3HMOVL),
         named_struct("TESTNAME","NP3PRSPR","TESTVALUE", NP3PRSPR),
         named_struct("TESTNAME","NP3PRSPL","TESTVALUE", NP3PRSPL),
         named_struct("TESTNAME","NP3TTAPR","TESTVALUE", NP3TTAPR),
         named_struct("TESTNAME","NP3TTAPL","TESTVALUE", NP3TTAPL),
         named_struct("TESTNAME","NP3LGAGR","TESTVALUE", NP3LGAGR),
         named_struct("TESTNAME","NP3LGAGL","TESTVALUE", NP3LGAGL),
         named_struct("TESTNAME","NP3RISNG","TESTVALUE", NP3RISNG),
         named_struct("TESTNAME","NP3GAIT","TESTVALUE", NP3GAIT),
         named_struct("TESTNAME","NP3FRZGT","TESTVALUE", NP3FRZGT),
         named_struct("TESTNAME","NP3PSTBL","TESTVALUE", NP3PSTBL),
         named_struct("TESTNAME","NP3POSTR","TESTVALUE", NP3POSTR),
         named_struct("TESTNAME","NP3BRADY","TESTVALUE", NP3BRADY),
         named_struct("TESTNAME","NP3PTRMR","TESTVALUE", NP3PTRMR),
         named_struct("TESTNAME","NP3PTRML","TESTVALUE", NP3PTRML),
         named_struct("TESTNAME","NP3KTRMR","TESTVALUE", NP3KTRMR),
         named_struct("TESTNAME","NP3KTRML","TESTVALUE", NP3KTRML),
         named_struct("TESTNAME","NP3RTARU","TESTVALUE", NP3RTARU),
         named_struct("TESTNAME","NP3RTALU","TESTVALUE", NP3RTALU),
         named_struct("TESTNAME","NP3RTARL","TESTVALUE", NP3RTARL),
         named_struct("TESTNAME","NP3RTALL","TESTVALUE", NP3RTALL),
         named_struct("TESTNAME","NP3RTALJ","TESTVALUE", NP3RTALJ),
         named_struct("TESTNAME","NP3RTCON","TESTVALUE", NP3RTCON),
         named_struct("TESTNAME","DYSKPRES","TESTVALUE", DYSKPRES),
         named_struct("TESTNAME","NHY","TESTVALUE", NHY)
      ) AS x
  FROM MDS_UPDRS_Part_III_Post_Dose_
) t1 LATERAL VIEW explode(x) t2 as y;

-- *****
DROP TABLE IF EXISTS Montreal_Cognitive_Assessment_MoCA;
CREATE EXTERNAL TABLE Montreal_Cognitive_Assessment_MoCA (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,
MCAALTTM INT,  MCACUBE INT, MCACLCKC INT, MCACLCKN INT, MCACLCKH INT, MCALION INT, MCARHINO INT,  MCACAMEL INT, MCAFDS INT, MCABDS INT, MCAVIGIL INT, MCASER7 INT, MCASNTNC INT, MCAVFNUM INT, MCAVF INT, MCAABSTR INT, MCAREC1 INT, MCAREC2 INT, MCAREC3 INT, MCAREC4 INT, MCAREC5 INT, MCADATE INT, MCAMONTH  INT, MCAYR INT, MCADAY  INT, MCAPLACE INT, MCACITY INT, MCATOT  INT, ORIG_ENTRY STRING, LAST_UPDATE STRING, QUERY STRING, SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '../data/Montreal_Cognitive_Assessment__MoCA_.csv'
OVERWRITE INTO TABLE Montreal_Cognitive_Assessment_MoCA;

DROP VIEW IF EXISTS MontrealCognitiveAssessmentMoCA;
CREATE VIEW MontrealCognitiveAssessmentMoCA 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","MCATOT","TESTVALUE", MCATOT)
         ) AS x
  FROM Montreal_Cognitive_Assessment_MoCA
) t1 LATERAL VIEW explode(x) t2 as y;

-- *****
DROP TABLE IF EXISTS Neurological_Exam_Cranial_Nerves;
CREATE EXTERNAL TABLE Neurological_Exam_Cranial_Nerves (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,
CN1RSP INT, CN1CM STRING, CN2RSP INT, CN2CM STRING, CN346RSP INT, CN346CM STRING, CN5RSP INT, CN5CM STRING, CN7RSP INT, CN7CM STRING, CN8RSP INT, CN8CM STRING, CN910RSP INT, CN910CM STRING, CN11RSP INT,CN11CM  STRING, CN12RSP INT, CN12CM STRING, ORIG_ENTRY STRING,  LAST_UPDATE STRING, QUERY STRING, SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../data/Neurological_Exam_-_Cranial_Nerves_.csv'
OVERWRITE INTO TABLE Neurological_Exam_Cranial_Nerves;

DROP VIEW IF EXISTS NeurologicalExamCranialNerves;
CREATE VIEW NeurologicalExamCranialNerves 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","CN1RSP","TESTVALUE", CN1RSP),
         named_struct("TESTNAME","CN2RSP","TESTVALUE", CN2RSP),
         named_struct("TESTNAME","CN346RSP","TESTVALUE", CN346RSP),
         named_struct("TESTNAME","CN5RSP","TESTVALUE", CN5RSP),
         named_struct("TESTNAME","CN7RSP","TESTVALUE", CN7RSP),
         named_struct("TESTNAME","CN8RSP","TESTVALUE", CN8RSP),
         named_struct("TESTNAME","CN910RSP","TESTVALUE", CN910RSP),
         named_struct("TESTNAME","CN11RSP","TESTVALUE", CN11RSP),
         named_struct("TESTNAME","CN12RSP","TESTVALUE", CN12RSP)
         ) AS x
  FROM Neurological_Exam_Cranial_Nerves
) t1 LATERAL VIEW explode(x) t2 as y;

-- *****
DROP TABLE IF EXISTS REM_Sleep_Disorder_Questionnaire;
CREATE EXTERNAL TABLE REM_Sleep_Disorder_Questionnaire (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,
PTCGBOTH INT, DRMVIVID INT, DRMAGRAC INT, DRMNOCTB INT, SLPLMBMV INT, SLPINJUR INT, DRMVERBL INT, DRMFIGHT INT, DRMUMV INT, DRMOBJFL INT, MVAWAKEN INT, DRMREMEM INT, SLPDSTRB INT, STROKE INT, HETRA INT, PARKISM INT, RLS INT, NARCLPSY  INT, DEPRS INT,EPILEPSY INT, BRNINFM INT, CNSOTH INT, CNSOTHCM  STRING,  ORIG_ENTRY STRING,   LAST_UPDATE STRING,  QUERY STRING, SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '../data/REM_Sleep_Disorder_Questionnaire.csv'
OVERWRITE INTO TABLE REM_Sleep_Disorder_Questionnaire;

DROP VIEW IF EXISTS REMSleepDisorderQuestionnaire;
CREATE VIEW REMSleepDisorderQuestionnaire 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","PTCGBOTH","TESTVALUE", PTCGBOTH),
         named_struct("TESTNAME","DRMVIVID","TESTVALUE", DRMVIVID),
         named_struct("TESTNAME","DRMAGRAC","TESTVALUE", DRMAGRAC),
         named_struct("TESTNAME","DRMNOCTB","TESTVALUE", DRMNOCTB),
         named_struct("TESTNAME","SLPLMBMV","TESTVALUE", SLPLMBMV),
         named_struct("TESTNAME","SLPINJUR","TESTVALUE", SLPINJUR),
         named_struct("TESTNAME","DRMVERBL","TESTVALUE", DRMVERBL),
         named_struct("TESTNAME","DRMFIGHT","TESTVALUE", DRMFIGHT),
         named_struct("TESTNAME","DRMUMV","TESTVALUE", DRMUMV),
         named_struct("TESTNAME","DRMOBJFL","TESTVALUE", DRMOBJFL),
         named_struct("TESTNAME","MVAWAKEN","TESTVALUE", MVAWAKEN),
         named_struct("TESTNAME","DRMREMEM","TESTVALUE", DRMREMEM),
         named_struct("TESTNAME","SLPDSTRB","TESTVALUE", SLPDSTRB),
         named_struct("TESTNAME","STROKE","TESTVALUE", STROKE),
         named_struct("TESTNAME","HETRA","TESTVALUE", HETRA),
         named_struct("TESTNAME","PARKISM","TESTVALUE", PARKISM),
         named_struct("TESTNAME","RLS","TESTVALUE", RLS),
         named_struct("TESTNAME","NARCLPSY","TESTVALUE", NARCLPSY),
         named_struct("TESTNAME","DEPRS","TESTVALUE", DEPRS),
         named_struct("TESTNAME","EPILEPSY","TESTVALUE", EPILEPSY),
         named_struct("TESTNAME","BRNINFM","TESTVALUE", BRNINFM),
         named_struct("TESTNAME","CNSOTH","TESTVALUE", CNSOTH)    
         ) AS x
  FROM REM_Sleep_Disorder_Questionnaire
) t1 LATERAL VIEW explode(x) t2 as y;


-- *****
DROP TABLE IF EXISTS Screening_Demographics;
CREATE EXTERNAL TABLE Screening_Demographics (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,
SIGNCNST INT,  
CONSNTDT STRING,
APPRDX INT, BIRTHDT INT, GENDER INT, HISPLAT INT, RAINDALS INT,  RAASIAN  INT, RABLACK INT, RAHAWOPI INT, RAWHITE INT, RANOS INT, RANOSCM STRING, PRJENRDT STRING, REFERRAL INT, DECLINED  STRING, RSNDEC  STRING, EXCLUDED INT, RSNEXC INT, ORIG_ENTRY STRING, LAST_UPDATE STRING, QUERY STRING,SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '../data/Screening_Demographics.csv'
OVERWRITE INTO TABLE Screening_Demographics;

DROP VIEW IF EXISTS ScreeningDemographics;
CREATE VIEW ScreeningDemographics 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","BIRTHDT","TESTVALUE", BIRTHDT),
         named_struct("TESTNAME","GENDER","TESTVALUE", GENDER),
         named_struct("TESTNAME","HISPLAT","TESTVALUE", HISPLAT),
         named_struct("TESTNAME","RAINDALS","TESTVALUE", RAINDALS),
         named_struct("TESTNAME","RAASIAN","TESTVALUE", RAASIAN),
         named_struct("TESTNAME","RABLACK","TESTVALUE", RABLACK),
         named_struct("TESTNAME","RAHAWOPI","TESTVALUE", RAHAWOPI),
         named_struct("TESTNAME","RAWHITE","TESTVALUE", RAWHITE),
         named_struct("TESTNAME","RANOS","TESTVALUE", RANOS)
         ) AS x
  FROM Screening_Demographics
) t1 LATERAL VIEW explode(x) t2 as y;


DROP TABLE IF EXISTS Socio_Economics;
CREATE EXTERNAL TABLE Socio_Economics (
REC_ID STRING,  
F_STATUS STRING,  
PATNO STRING, 
EVENT_ID STRING,  
PAG_NAME STRING,  
INFODT STRING,
EDUCYRS INT, HANDED INT, ORIG_ENTRY STRING, LAST_UPDATE STRING, QUERY STRING, SITE_APRV STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
tblproperties("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '../data/Socio_Economics.csv'
OVERWRITE INTO TABLE Socio_Economics;

DROP VIEW IF EXISTS SocioEconomics;
CREATE VIEW SocioEconomics 
AS
SELECT PATNO, y.TESTNAME, y.TESTVALUE 
FROM (
  SELECT 
    PATNO,  
    array (
         named_struct("TESTNAME","EDUCYRS","TESTVALUE", EDUCYRS),
         named_struct("TESTNAME","HANDED","TESTVALUE", HANDED)
                  ) AS x
  FROM Socio_Economics
) t1 LATERAL VIEW explode(x) t2 as y;
 

DROP VIEW IF EXISTS biomarkers_P2ALL_PD;
CREATE VIEW biomarkers_P2ALL_PD 
AS
SELECT * FROM famHistPD
UNION ALL
SELECT * FROM GenNeurologicalExam
UNION ALL
SELECT * FROM MDSUPDRSPartIIIPostDose
UNION ALL
SELECT * FROM MontrealCognitiveAssessmentMoCA
UNION ALL
SELECT * FROM  NeurologicalExamCranialNerves
UNION ALL
SELECT * FROM REMSleepDisorderQuestionnaire
UNION ALL
SELECT * FROM ScreeningDemographics
UNION ALL
SELECT * FROM SocioEconomics;

INSERT OVERWRITE LOCAL DIRECTORY '../data/biomarkersP2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT biomarkers_P2ALL_PD.PATNO, biomarkers_P2ALL_PD.TESTNAME, biomarkers_P2ALL_PD.TESTVALUE, biospec.DIAGNOSIS
FROM biomarkers_P2ALL_PD 
LEFT OUTER JOIN biospec
ON biomarkers_P2ALL_PD.PATNO = biospec.PATNO
WHERE biospec.DIAGNOSIS IS NOT NULL
AND biomarkers_P2ALL_PD.TESTVALUE IS NOT NULL


