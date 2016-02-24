/**
 * Created by selvan on 16/02/2016.
 */
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Giri R Varatharajan on 9/8/2015.
 * https://hadoopist.wordpress.com/
 */
object SparkWordCount {
  def main(args:Array[String]) : Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val tf = sc.textFile(args(0))
    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
    val counts = splits.reduceByKey((x,y)=>x + y)
    splits.saveAsTextFile(args(1))
    counts.saveAsTextFile(args(2))
  }
  //D:\Spark\spark-1.5.0-bin-hadoop2.6\bin>spark-submit --class SparkWordCount --master local[*] D:\typesafe-activator-1.3.7-minimal\activator-1.3.7-minimal\MyFirstSparkProject\target\scala-2.10\myfirstsparkproject_2.10-1.0.jar D:\Spark\spark-1.6.0\README.md D:\Spark\spark-1.6.0\CountOutput D:\Spark\spark-1.6.0\SplitOutput

}

