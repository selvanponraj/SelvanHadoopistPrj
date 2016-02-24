
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by selvan on 17/02/2016.
 * https://hadoopist.wordpress.com/
 */
object WordCountDS {
  def main(args:Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    //With Spark RDD Core-APIs
        val tf = sc.textFile(args(0))
        val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1)).filter(_ != "")
        val counts = splits.reduceByKey((x,y)=>x + y).take(10000).foreach(println)
  }
}
