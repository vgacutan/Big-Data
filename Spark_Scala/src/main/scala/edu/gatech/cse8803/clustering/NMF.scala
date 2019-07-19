package edu.gatech.cse8803.clustering

/**
  * @author Hang Su <hangsu@gatech.edu>
  */


import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix




object NMF {

  /**
   * Run NMF clustering 
   * @param V The original non-negative matrix 
   * @param k The number of clusters to be formed, also the number of cols in W and number of rows in H
   * @param maxIterations The maximum number of iterations to perform
   * @param convergenceTol The maximum change in error at which convergence occurs.
   * @return two matrixes W and H in RowMatrix and DenseMatrix format respectively 
   */
  def run(V: RowMatrix, k: Int, maxIterations: Int, convergenceTol: Double = 1e-4): (RowMatrix, BDM[Double]) = {

      /**
     * TODO 1: Implement your code here
     * Initialize W, H randomly 
     * Calculate the initial error (Euclidean distance between V and W * H)
     */

    var W = new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache)
    var H = BDM.rand[Double](k, V.numCols().toInt)

    val sc=W.rows.sparkContext
    val WHml =  multiply(W,H)
    var error = errorRate(V,WHml)

    var intIter=0

    while(error > convergenceTol && intIter<maxIterations){

      W.rows.cache()
      V.rows.cache()

      sc.broadcast(H * H.t)
      val Wmt = multiply(W,(H * H.t))
      val Vmt = multiply(V,H.t)
      val WVmt = dotProd(W,Vmt)
      val WHmt = multiply(W,H)
      W = dotDiv(WVmt,Wmt)

      sc.broadcast(computeWTV(W,W))
      val WVm = computeWTV(W,V)

      H = (H :* WVm) :/ (computeWTV(W,W) * H)

      val WHm = multiply(W,H)
      error = errorRate(V,WHm)
      W.rows.unpersist(false)
      V.rows.unpersist(false)
    }

    /**
     * TODO 2: Implement your code here
     * Iteratively update W, H in a parallel fashion until error falls below the tolerance value 
     * The updating equations are, 
     * H = H.* W^T^V ./ (W^T^W H)
     * W = W.* VH^T^ ./ (W H H^T^)
     */

    (W,H)
  }
    def errorRate(V: RowMatrix,WH: RowMatrix): Double = {
        val Vr = V.rows.zip(WH.rows)
        val Vm = Vr.map(f => toBreezeVector(f._1) :- toBreezeVector(f._2))
        val Vres = Vm.map(f => f :* f).map(f => sum(f)).reduce(_+_)/2
        Vres

  }

  /** TODO: Remove the placeholder for return and replace with correct values */
    //(new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache), BDM.rand[Double](k, V.numCols().toInt))


  /**  
  * RECOMMENDED: Implement the helper functions if you needed
  * Below are recommended helper functions for matrix manipulation
  * For the implementation of the first three helper functions (with a null return), 
  * you can refer to dotProd and dotDiv whose implementation are provided
  */
  /**
  * Note:You can find some helper functions to convert vectors and matrices
  * from breeze library to mllib library and vice versa in package.scala
  */

  /** compute the mutiplication of a RowMatrix and a dense matrix */
  /**
  private def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {
    null
  } */
  def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {
    X.multiply(fromBreeze(d))
  }


 /** get the dense matrix representation for a RowMatrix */
 /**
  private def getDenseMatrix(X: RowMatrix): BDM[Double] = {
    null
  } */
 def getDenseMatrix(X: RowMatrix): BDM[Double] = {
   val gdm = X.rows.map{ m=> new BDM[Double](1,m.size,m.toArray)
   }
   val getMttrx = gdm.reduce((a,b)=>DenseMatrix.vertcat(a,b))
   getMttrx
 }


  /** matrix multiplication of W.t and V */
  /**def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {
    null
  } */
  def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {

    getDenseMatrix(W).t * getDenseMatrix(V)

  }

  /** dot product of two RowMatrixes */
  def dotProd(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

}


