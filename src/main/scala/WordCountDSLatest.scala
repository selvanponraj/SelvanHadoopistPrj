import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
/**
 * Created by selvan on 17/02/2016.
 * https://hadoopist.wordpress.com/
 */
object WordCountDSLatest {
  def main(args:Array[String]) : Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("DataSet Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //With Spark DataSets API
    //Since the Dataset version of word count can take advantage of the built-in aggregate count,
    // this computation can not only be expressed with less code, but it will also execute significantly faster.
        import sqlContext.implicits._
        val tf = sqlContext.read.text(args(0)).as[String]
        val splits = tf.flatMap(_.split(" ")).filter(_ !=" ")
        val counts = splits.groupBy(_.toLowerCase).count().take(10000).foreach(println)

    //splits.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\SplitOutput")
    //counts.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\CountOutput")
  }

}
