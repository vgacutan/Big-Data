/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

object T2dmPhenotype {
  
  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */


  def abn_labTest(res: LabResult): Boolean = {
    res.testName match{
      case "hba1c" => res.value >= 6
      case "hemoglobin a1c" => res.value >= 6
      case "Fasting glucose" => res.value >= 110
      case "Fasting blood glucose" => res.value >= 110
      case "fasting plasma glucose" => res.value >= 110
      case "Glucose" => res.value >= 110
      case "glucose" => res.value >= 110
      case "Glucose, serum" => res.value >= 110
      case   _=> false
    }
  }

  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext

    /** Hard code the criteria */

    val t1DM_diag = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")
    val t1DM_rxMedic = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")
    val t2DM_diag = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")
    val t2DM_rxMedic = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")
    val DM_RELATED_DX = Set("790.21","790.22","790.2","790.29","648.81","648.82","648.83","648.84","648","648.01","648.02","648.03","648.04","791.5","277.7","V77.1","256.4")

    /** Find CASE Patients */
    val case_p1 = diagnostic.filter(col => !t1DM_diag.contains(col.code) && t2DM_diag.contains(col.code)) //.map(x => x.patientID).distinct()
    val case_p1_dist = case_p1.map(col => col.patientID).distinct()
    //val Med1 = medication.filter(col => t1DM_rxMedic.contains(col.medicine.toLowerCase))
    //val Med2 = medication.filter(col => t2DM_rxMedic.contains(col.medicine.toLowerCase))
    val caseN_pt = case_p1_dist.subtract(medication.filter(col => t1DM_rxMedic.contains(col.medicine.toLowerCase)).map(col=>col.patientID))
    //println("CASE1: ", caseN_pt.collect.toSet.size)

    val caseN_p2 = case_p1_dist.intersection(medication.filter(col => t1DM_rxMedic.contains(col.medicine.toLowerCase)).map(line=>line.patientID)).subtract(medication.filter(col => t2DM_rxMedic.contains(col.medicine.toLowerCase)).map(line=>line.patientID))
    //println("CASE2: ", caseN_p2.collect.toSet.size)

    val caseY_p3 = medication.filter(col => t1DM_rxMedic.contains(col.medicine.toLowerCase)).groupBy(col=>col.patientID)//.map(x=>(x._1,x._2.minBy(y=>y.date).date))
    val caseY_p3_md = caseY_p3.map(l=>(l._1,l._2.minBy(x=>x.date).date))

    val caseY_p3_1 = medication.filter(col => t2DM_rxMedic.contains(col.medicine.toLowerCase)).groupBy(line=>line.patientID)//.map(x=>(x._1,x._2.minBy(y=>y.date).date))
    val caseY_p3_2 = caseY_p3_1.map(l=>(l._1,l._2.minBy(x=>x.date).date))

    val caseY_p3_jn = caseY_p3_2.join(caseY_p3_md).filter(col=>col._2._1.before(col._2._2))//.map(x=>x._1)
    val caseY_p3_join = caseY_p3_jn.map(x=>x._1)
    val caseY_pt3 = case_p1_dist.intersection(caseY_p3_join)
    //println("CASE3: ", caseY_pt3.collect.toSet.size)

    val caseAllPT = caseN_pt.union(caseN_p2).union(caseY_pt3).distinct().map(x=>(x,1))
    //println("caseAllPT: ", caseAllPT.collect.toSet.size)

    /** Find CONTROL Patients */
    val DiabAll = diagnostic.filter(line=> DM_RELATED_DX.contains(line.code)||line.code.contains("250.")).map(line=>line.patientID)
    val control_pt = labResult.filter(col=>(col.testName.toLowerCase.contains("glucose") )).filter(col=> !abn_labTest(col)).map(x=>x.patientID).subtract(DiabAll).distinct().map(col=>(col,2))
    //println("CONTROL: ", control_pt.collect.toSet.size)

    /** Find OTHER Patients */
    val medic_Pid =  medication.map(col=>col.patientID)
    val labRes_Pid = labResult.map(col=>col.patientID)
    val diag_Pid = diagnostic.map(col=>col.patientID)

    val Other_pt = medic_Pid.union(labRes_Pid).union(diag_Pid)

    val Other_pt_rem = Other_pt.subtract(caseAllPT.map(col=>col._1)).subtract(control_pt.map(x=>x._1))//.map(y=>(y,3))
    val Other_pt_All = Other_pt_rem.map(y=>(y,3))
    //println("OTHER: ", Other_pt_All.collect.toSet.size)

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(caseAllPT, control_pt, Other_pt_All).distinct()
    //println("phenotypeLabel: ", phenotypeLabel.collect.toSet.size)
    //println("phenotypeLabel: ", phenotypeLabel.take(50).foreach(println))


    /** Return */
    phenotypeLabel
  }
}
