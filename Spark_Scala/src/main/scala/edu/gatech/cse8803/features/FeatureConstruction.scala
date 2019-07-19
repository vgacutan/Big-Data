/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val diagn_eve = diagnostic.map(d => ((d.patientID, d.code), 1.0))
    val diagnostic_eve = diagn_eve.reduceByKey(_ + _)
    //println("1_DiagnosticFeatureT: ", diagnostic_eve.take(5).foreach(println))
    diagnostic_eve
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val medic_eve = medication.map(m => ((m.patientID, m.medicine), 1.0))
    val medication_eve = medic_eve.reduceByKey(_ + _)
    //println("1_medication_eveFeatureT: ", medication_eve.take(5).foreach(println))
    medication_eve

  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val labRes = labResult.map(col => ((col.patientID, col.testName), (col.value, 1)))
    val labRes_aggr = labRes.reduceByKey((a, b) => ((a._1 + b._1), (a._2 + b._2)))
    val labRes_eve = labRes_aggr.map(l => (l._1, l._2._1 / l._2._2))
    //println("LabFeatureT: ", labRes_eve.take(5).foreach(println))
    labRes_eve

  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val candDiag = candiateCode.map(_.toLowerCase)
    val diagn_eve1 = diagnostic.filter(c => candDiag.contains(c.code.toLowerCase))
    val diagn_mp = diagn_eve1.map(m => ((m.patientID, m.code), 1.0))
    val diagnFeat_eve = diagn_mp.reduceByKey(_ + _)
    //println("DiagnosticFeatureTuple: ", diagnFeat_eve.take(5).foreach(println))
    diagnFeat_eve

  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val candMed = candidateMedication.map(_.toLowerCase)
    val cMed = medication.filter(d => (candMed.contains(d.medicine.toLowerCase)))
    val medic_eve1 = cMed.map(d => ((d.patientID, d.medicine), 1.0))
    val medicFeat_eve = medic_eve1.reduceByKey(_ + _)
    //println("MedicationFeatureTuple: ", medicFeat_eve.take(5).foreach(println))
    medicFeat_eve
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val cLab = candidateLab.map(_.toLowerCase)
    val labRes1 = labResult.filter(col=>cLab.contains(col.testName.toLowerCase))
    val labRes1_eve = labRes1.map(col => ((col.patientID, col.testName), (col.value, 1)))
    val labRes1_aggr = labRes1_eve.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val labResFeat_eve = labRes1_aggr.map(l => (l._1, l._2._1 / l._2._2))
    //println("LabFeatureTuple: ", labResFeat_eve.take(5).foreach(println))
    labResFeat_eve

  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map*/

    val patientFeatureNames = feature.map(_._1._2).distinct().collect()
    val patientFeatureNamesInd = patientFeatureNames.zipWithIndex.toMap
    val patientFeatures = sc.broadcast(patientFeatureNamesInd)

    /**
     * Functions maybe helpful:
     *    collect
     *    groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val featureMapEve = feature.map(l => (l._1._1, (l._1._2, l._2))).groupByKey()
    val result = featureMapEve.map(feat => {
      val features = feat._2.map({case(fName, fval) => (patientFeatures.value(fName), fval)})
      val vec = Vectors.sparse(patientFeatures.value.size, features.toSeq)
      (feat._1, vec)
    })
    /**
    val result = sc.parallelize(Seq(("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))  */

    result
    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */

  }
}


