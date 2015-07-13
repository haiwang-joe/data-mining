import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.rdd.RDD
import java.io._

object MultipleMatrixProd {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("MatrixMultiply").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    def getPair(s: String, mType: String = "row") = {
      val infos = s.split('\t')
      val value = infos(1).toDouble
      val firstPart = infos(0).split(',')
      val row = firstPart(0).toInt
      val col = firstPart(1).toInt
      if (mType == "row") (row, (col, value))
      else (col, (row, value))
    }
    def getMatrix(file: String) = sc.textFile(file).map(x => getPair(x)).groupByKey(4)
    def getMatrixCol(file: String) = sc.textFile(file).map(x => getPair(x, mType = "col")).groupByKey(1000)
    def rowColItemProd(a: Iterable[(Int, Double)], b: Iterable[(Int, Double)]): Double = {
      val keyIntersect = a.map(_._1).toSet intersect b.map(_._1).toSet
      val am = a.toMap
      val bm = b.toMap
      var result = 0D
      for (key <- keyIntersect) {
        result += am(key) * bm(key)
      }
      result
    }

    def getIndexedRow(a: (Int, Iterable[(Int, Double)])): IndexedRow = {
      val vector = Vectors.sparse(57358, a._2.toSeq)
      new IndexedRow(a._1, vector)
    }

    def multiplyMat(a: RDD[(Int, Iterable[(Int, Double)])], b: RDD[(Int, Iterable[(Int, Double)])], resultName: String) {
      val bPartitions = b.partitions
      var partNum = 0
      for (p <- bPartitions) {
        partNum += 1
        val idx = p.index
        val partB = b.mapPartitionsWithIndex((pIdx, partition) => if (pIdx == idx) partition else Iterator(), true)
        val partBInArray = partB.collect()
        println("partNum: " + partNum)
        val pBroadcast = sc.broadcast(partBInArray)
        // rowColItemProd(row._2, col._2) computed twice here, to be optimize
        val partialResult = a.mapPartitions(
          iter => {
            iter.flatMap(row =>
              pBroadcast.value.filter {
                col => rowColItemProd(row._2, col._2) != 0D
              }.map(col => (row._1, (col._1, rowColItemProd(row._2, col._2)))))
          }, true)
        partialResult.saveAsTextFile("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_" + resultName + "_" + partNum + ".out")
      }

    }

    val mgi = getMatrix("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_g_i.out").cache()
    val mip = getMatrixCol("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_i_p.out").cache()

    multiplyMat(mgi, mip, "g_p")

    val mgp = getMatrix("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_g_p_*").cache()

    // getMatrixCol should be replaced by getMatrix if the data format in matrix_p_g.out is (g,p "\t" value)
    val mpg = getMatrixCol("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_p_g.out").cache()

    multiplyMat(mgp, mpg, "g_g")

    val gg = getMatrixCol("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_g_g_*").cache()

    multiplyMat(gg, gg, "z")
//    val z = getMatrix("hdfs://hdbm0.hy01:54310/user/fangyuanli/data/matrix_z_*").cache()
//
//    // convert Rdd to Rdd[IndexRow]
//    val zMatrix = new IndexedRowMatrix(z.map(getIndexedRow))
//
//    val arr = Array.fill(57358)(0.5)
//    var g = Matrices.dense(57358, 1, arr)
//
//    while (g.toArray.maxBy(x => x) >= 0.01) {
//      val tmpArr = zMatrix.multiply(g).rows.map(x => x.vector.toArray(0)).collect()
//      val absSum = tmpArr.reduce((x, y) => x.abs + y.abs)
//      g = Matrices.dense(57358, 1, tmpArr.map(x => x / absSum))
//
//    }
//
//    val output = new PrintWriter(new File("~/result"))
//    for(item <- g.toArray){
//      output.println(item)
//    }
//
//    output.close()

    // Exit
    sc.stop()
  }
}
