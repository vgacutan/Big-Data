

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import java.text.{NumberFormat, SimpleDateFormat}
//import java.util.{Date, Locale}
//import java.util.concurrent.TimeUnit

import edu.gatech.cse8803.clustering.{NMF, Metrics}
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
//import scala.reflect.internal.util.TableDef.Column


object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)

    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData


    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)
    //println("rawFeatures: ", rawFeatures.take(20).foreach(println))


    val (kMeansPurity, gaussianMixturePurity, streamKmeansPurity, nmfPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of StreamingKMeans is: $streamKmeansPurity%.5f")
    println(f"[All feature] purity of NMF is: $nmfPurity%.5f")


    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)
    //println("filteredRawFeatures: ", filteredRawFeatures.take(20).foreach(println))


    val (kMeansPurity2, gaussianMixturePurity2, streamKmeansPurity2, nmfPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of StreamingKMeans is: $streamKmeansPurity2%.5f")
    println(f"[Filtered feature] purity of NMF is: $nmfPurity2%.5f")

    sc.stop

  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures:RDD[(String, Vector)]): (Double, Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})
    val rawFeatureVectors = features.map(_._2).cache()

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]
    /** transform a feature into its reduced dimension representation */
    def transform(feature: Vector): Vector = {
      Vectors.dense(Matrices.dense(1, feature.size, feature.toArray).multiply(densePc).toArray)
    }

    /** TODO: K Means Clustering using spark mllib
      *  Train a k means model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/


    featureVectors.cache()
    val numK = 3
    val mxIter = 20
    val testSeed = 8803L

    val kM_mod = KMeans.train(featureVectors,numK,mxIter,1,"k-means||",testSeed).predict(featureVectors)
    val kM_feat = rawFeatures.map(_._1).zip(kM_mod)
    val kM_grp = kM_feat.join(phenotypeLabel).map(_._2)

    var kTable = kM_grp.filter( x => x._1 == 1 ).map( x => x._2).countByValue()
    var k_Table = kM_grp.countByValue()

    val kMeansPurity = Metrics.purity(kM_grp)

    //val kMeansPurity = 0.0

    /** TODO: GMMM Clustering using spark mllib
      *  Train a Gaussian Mixture model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/

    val gmm = new GaussianMixture().setK(numK).setMaxIterations(mxIter).setSeed(testSeed)
    val gmm_mod = gmm.run(featureVectors)
    val gmmPred = gmm_mod.predict(featureVectors).zip(features).map(pred => (pred._2._1, pred._1))
    val gmmPredAll = gmmPred.join(phenotypeLabel).map(pred => (pred._2._1 + 1, pred._2._2))//.cache()


    val gaussianMixturePurity = Metrics.purity(gmmPredAll)

    //val gaussianMixturePurity = 0.0

    /** TODO: StreamingKMeans Clustering using spark mllib
      *  Train a StreamingKMeans model using the variabe featureVectors as input
      *  Set the number of cluster K = 3 and DecayFactor = 1.0 and weight as 0.0
      *  please pay attention to the input type
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/

    val decFactor = 1.0
    val numOfDim = 10

    val str_Kmeans = new StreamingKMeans().setK(numK).setDecayFactor(decFactor).setRandomCenters(numOfDim,0.0,testSeed)

    val str_KMod = str_Kmeans.latestModel()
    val str_KModUpd =  str_KMod.update(featureVectors, str_Kmeans.decayFactor, str_Kmeans.timeUnit)

    val str_Kmeans_pred = str_KModUpd.predict(featureVectors)
    val str_KmeansPredic = str_Kmeans_pred.zip(features).map(str => (str._2._1, str._1))
    val str_KmeansPredicAll =   str_KmeansPredic.join(phenotypeLabel).map(str => (str._2._1 + 1, str._2._2))


    val streamKmeansPurity = Metrics.purity(str_KmeansPredicAll)

    //val streamKmeansPurity = 0.0


    /** NMF */
    val rawFeaturesNonnegative = rawFeatures.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)
    // zip patientIDs with their corresponding cluster assignments
    // Note that map doesn't change the order of rows
    val assignmentsWithPatientIds=features.map({case (patientId,f)=>patientId}).zip(assignments) 
    // join your cluster assignments and phenotypeLabel on the patientID and obtain a RDD[(Int,Int)]
    // which is a RDD of (clusterNumber, phenotypeLabel) pairs
    val nmfClusterAssignmentAndLabel = assignmentsWithPatientIds.join(phenotypeLabel).map({case (patientID,value)=>value})
    // Obtain purity value
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)

    /**
    println("nmfClusterAssignmentAndLabel******************************")
    println(nmfClusterAssignmentAndLabel.take(20).foreach(println)) */

    (kMeansPurity, gaussianMixturePurity, streamKmeansPurity, nmfPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
    *
    * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /** You may need to use this date format. */
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    /** load data using Spark SQL into three RDDs and return them
      * Hint: You can utilize edu.gatech.cse8803.ioutils.CSVUtils and SQLContext.
      *
      * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
      *       Be careful when you deal with String and numbers in String type.
      *       Ignore lab results with missing (empty or NaN) values when these are read in.
      *       For dates, use Date_Resulted for labResults and Order_Date for medication.
      * */

    /** TODO: implement your own code here and remove existing placeholder code below */


    val md = CSVUtils.loadCSVAsTable(sqlContext, "data/medication_orders_INPUT.csv", "medic_ord")
    //val medRddRows = sqlContext.sql("SELECT Member_ID,Order_Date,Drug_Name FROM medicine")
    val medication: RDD[Medication] = sqlContext.sql("SELECT Member_ID,Order_Date,Drug_Name FROM medic_ord").map(col => Medication(col(0).toString, dateFormat.parse(col(1).toString), col(2).toString.toLowerCase))
    //println("medication: ", medication.take(10).foreach(println))

    val lb = CSVUtils.loadCSVAsTable(sqlContext,"data/lab_results_INPUT.csv","lab_res")
    //val labrowsSQL = sqlContext.sql("SELECT Member_ID, Date_Resulted, Result_Name, Numeric_Result FROM lab WHERE Numeric_Result!=''")
    val labResult: RDD[LabResult] =  sqlContext.sql("SELECT Member_ID, Date_Resulted, Result_Name, Numeric_Result FROM lab_res WHERE Numeric_Result!=''").map(col=>LabResult(col(0).toString, dateFormat.parse(col(1).toString), col(2).toString, col(3).toString.filterNot(",".toSet).toDouble))
    //println("labResult: ", labResult.take(10).foreach(println))

    val ei = CSVUtils.loadCSVAsTable(sqlContext,"data/encounter_INPUT.csv","encInput")
    val ei_icdCode = CSVUtils.loadCSVAsTable(sqlContext,"data/encounter_dx_INPUT.csv","encInput_ICD")
    //val diagSQL = sqlContext.sql("SELECT Member_ID, Encounter_ID, Encounter_DateTime FROM diag")
    //val codeSQL = sqlContext.sql("SELECT Encounter_ID, code FROM ICD")

    //val diagICD = sqlContext.sql("SELECT d.Member_ID, d.Encounter_DateTime, i.code FROM diag d JOIN ICD i on d.Encounter_ID = i.Encounter_ID")
    val diagnostic: RDD[Diagnostic] =  sqlContext.sql("SELECT encInput.Member_ID, encInput.Encounter_DateTime, encInput_ICD.code FROM encInput  LEFT JOIN encInput_ICD  on encInput.Encounter_ID = encInput_ICD.Encounter_ID").map(col=> Diagnostic(col(0).toString, dateFormat.parse(col(1).toString), col(2).toString))
    //println("diagnostic: ", diagnostic.take(10).foreach(println))


    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
