/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.clustering

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   *             \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return purity
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {

    /**
     * TODO: Remove the placeholder and implement your code here
     */

    val clustMet = clusterAssignmentAndLabel.map(col => ((col._1, col._2), 1))
    val clustMetAdd = clustMet.keyBy(_._1).reduceByKey((i, j) => (i._1, i._2 + j._2))
    val clustMetmap = clustMetAdd.map(c => (c._2._1._1, c._2._2))
    val clustMetmx = clustMetmap.keyBy(_._1).reduceByKey((i, j) => (1, math.max(i._2, j._2)))
    val clustMetmap2 = clustMetmx.map(c => c._2._2)
    val clustMetAssign = clustMetmap2.reduce(_+_)/clusterAssignmentAndLabel.count().toDouble
    clustMetAssign

    //0.0
  }
}
