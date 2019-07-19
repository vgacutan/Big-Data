-- ETL for Final Project
-- Latest Fixes:  DISTINCT LABELS and PROPER NORMALIZATION

-- register a python UDF for converting data into SVMLight format
REGISTER pig/utils.py USING jython AS utils;

-- load data with and specify column names and covert to desired datatypes
biotests = LOAD '../data/biospecimens.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE'
, 'UNIX', 'SKIP_INPUT_HEADER') AS (PATNO:int, GENDER:chararray, DIAGNOSIS:chararray, CLINICAL_EVENT:chararray,
TYPE:chararray, TESTNAME:chararray, TESTVALUE:float, UNITS:chararray,
RUNDATE: chararray, PROJECTID:int, PI_NAME:chararray, PI_INSTITUTION:chararray, update_stamp:chararray);

biotestsP2 = LOAD '../data/biomarkersP2/' USING PigStorage(',') AS (PATNO:int, TESTNAME:chararray, TESTVALUE:float, DIAGNOSIS:chararray);


-- Select columns to use
bioSelections = FOREACH biotests GENERATE PATNO, TESTNAME, TESTVALUE, DIAGNOSIS;
bioSelectionsP2 = FOREACH biotestsP2 GENERATE PATNO, TESTNAME, TESTVALUE, DIAGNOSIS;

-- Drop rows where TESTVALUE is NULL.
bioSelections = FILTER bioSelections BY TESTVALUE IS NOT NULL;
bioSelectionsP2 = FILTER bioSelectionsP2 BY TESTVALUE IS NOT NULL;

tmp = LIMIT bioSelectionsP2 10;
DUMP tmp


-- Combine biomarkers
bioSelectionsALL = UNION bioSelections, bioSelectionsP2;



-- labels = create it of the form (patientid, label) for dead and alive patients

labels = FOREACH bioSelectionsALL GENERATE PATNO, (DIAGNOSIS=='Control'?0:1) as label;
labels = DISTINCT labels;


-- ***************************************************************************
-- Generate feature mapping
-- ***************************************************************************


-- map features
all_features = FOREACH bioSelectionsALL GENERATE TESTNAME;
all_features = DISTINCT all_features;
all_features = ORDER all_features BY TESTNAME;
all_features = RANK all_features BY TESTNAME;
all_features = FOREACH all_features GENERATE ((long)$0 - 1L) as idx, TESTNAME;


-- store the features as an output file


STORE all_features INTO '../data/features' using PigStorage(',');


-- features = perform join of featureswithid and all_features by eventid and replace eventid with idx.
-- It is of the form (patientid, idx, featurevalue).


-- replace testname with new index
features = JOIN bioSelectionsALL BY TESTNAME LEFT OUTER, all_features by TESTNAME;
features = FOREACH features GENERATE bioSelectionsALL::PATNO as PATNO,
                                     all_features::idx as idx,
                                     bioSelectionsALL::TESTVALUE as TESTVALUE;


-- deal with duplicates (take the average)

features = GROUP features BY (PATNO,idx);
features = FOREACH features GENERATE FLATTEN(group), AVG(features.TESTVALUE) as TESTVALUE;


features = ORDER features BY PATNO, idx;


STORE features INTO '../data/features_map' USING PigStorage(',');


-- ***************************************************************************
-- Export Unnormalized features with labels
-- ***************************************************************************

labeledrawfeatures = JOIN features BY PATNO, labels BY PATNO;
labeledrawfeatures = FOREACH labeledrawfeatures GENERATE features::group::PATNO as PATNO,
                                                         features::group::idx as idx,
                                                         features::TESTVALUE as TESTVALUE,
                                                         labels::label as label;



labeledrawfeatures = ORDER labeledrawfeatures BY PATNO, idx;


STORE labeledrawfeatures INTO '../data/labeled_raw_features' USING PigStorage(',');


-- ***************************************************************************
-- Normalize the values using min-max normalization
-- ***************************************************************************

-- maxvalues = group events by idx and compute the maximum feature value in each group.
-- It is of the form (idx, maxvalues)

-- normalization
featuresgrpd = GROUP features BY idx;


maxvalues = FOREACH featuresgrpd GENERATE flatten(group) as idx, MAX(features.TESTVALUE) as maxvalue;


-- normalized = join features and maxvalues by idx

normalized = JOIN features BY idx LEFT OUTER, maxvalues by idx;

-- features = compute the final set of normalized features of the form (patientid, idx, normalizedfeaturevalue)
features = FOREACH normalized GENERATE features::group::PATNO as PATNO, features::group::idx as idx,
                   (double)features::TESTVALUE / (double)maxvalues::maxvalue as normalizedfeaturevalue;

features = FILTER features BY normalizedfeaturevalue IS NOT NULL;



features = ORDER features BY PATNO, idx;


STORE features INTO '../data/features_normalized' USING PigStorage(',');



-- ***************************************************************************
-- Generate features in svmlight format
-- features is of the form (patientid, idx, normalizedfeaturevalue) and is the output of the previous step

grpd = GROUP features BY PATNO;
grpd_order = ORDER grpd BY $0;
features = FOREACH grpd_order
{
        sorted = ORDER features BY idx;
        generate group as PATNO, utils.bag_to_svmlight(sorted) as sparsefeature;
}


-- ***************************************************************************
-- Split into train and test set
-- labels is of the form (patientid, label) and contains all patientids followed by label of 1 for dead and 0 for alive
-- e.g. 1,1
--	    2,0
--      3,1
-- ***************************************************************************

--Generate sparsefeature vector relation

samples = JOIN features BY PATNO, labels BY PATNO;
samples = DISTINCT samples PARALLEL 1;
samples = ORDER samples BY $0;
samples = FOREACH samples GENERATE $3 AS label, $1 AS sparsefeature;

--TEST-6

STORE samples INTO '../data/samples' USING PigStorage(' ');

-- randomly split data for training and testing
DEFINE rand_gen RANDOM('6505');
samples = FOREACH samples GENERATE rand_gen() as assignmentkey, *;
SPLIT samples INTO testing IF assignmentkey <= 0.20, training OTHERWISE;
training = FOREACH training GENERATE $1..;
testing = FOREACH testing GENERATE $1..;

-- save training and tesing data

STORE testing INTO '../data/testing' USING PigStorage(' ');
STORE training INTO '../data/training' USING PigStorage(' ');

