import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by selvan on 19/02/2016.
 * //http://stackoverflow.com/questions/30194150/compare-two-files-content-in-scala-and-spark/30195664#30195664
 */
object CompareTwoFiles {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("prep").setMaster("local")
    val sc = new SparkContext(conf)
    val searchList = sc.textFile("src/main/resources/data/words.txt")

    val sentilex = sc.broadcast(searchList.map({ (line) =>
      val Array(a,b) = line.split(",").map(_.trim)
      (a,b.toInt)
    }).collect().toMap)

    val sample1 = sc.textFile("src/main/resources/data/data.txt")
    val sample2 = sample1.map(line=>(line.split(" ").map(word => sentilex.value.getOrElse(word, 0)).reduce(_ + _), line))
    sample2.collect.foreach(tuple=>println(tuple._1,tuple._2))
  }

  /**
   *  Expected Output
   *
   *
   -2 ; surprise heard thump opened door small seedy man clasping package wrapped.

 1 ; upgrading system found review spring 2008 issue moody audio mortgage backed.

 2 ; omg left gotta wrap review order asap . understand hand delivered dali lama

 0 ; speak hands wear earplugs lives . listen maintain link long .

 -2 ; buffered lightning thousand volts cables burned revivification place .

 1 ; cables cables finally able hear auditory gem long rumored music .
   */
}
