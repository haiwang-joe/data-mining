import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}

object Fim {
    def rearrange(s: String): (String, String) = {
        val t = s.split('\t')
        (t(0), t(1))
    }
    def legalrecord(s: String): Boolean = {
        val a = s.split('\t')
        a.length == 2 && a(0) != "-" && a(1) != "-" && a(1) != "" && a(0) != "" && a(1) != "Default"
    }
    def main(args: Array[String]) {
        // args: [minSupportCount, numPartitions, inputFile, outputFile]
        val sc = new SparkContext(new SparkConf())

        // Load and parse the data
        val data = sc.textFile(args(2))
        val transaction0 = data.filter(legalrecord)
        val transaction1 = transaction0.map(rearrange)
        val transaction2 = transaction1.groupByKey()
        //val transactions = transaction2.map(s => scala.collection.immutable.Set(s._2).toArray)
        val transactions = transaction2.map(s => s._2.toSet.toArray)

        val minSupport = args(0).toDouble / transactions.count() 
        val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(args(1).toInt)
        val model = fpg.run(transactions)

        val output = model.freqItemsets.map(itemset => itemset.items.mkString("\t") + "\t" + itemset.freq)
        output.saveAsTextFile(args(3))
        sc.stop()
    }
}
