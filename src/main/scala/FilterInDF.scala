import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql.functions._

/**
 * Created by selvan on 22/02/2016.
 */
object FilterInDF extends App{

      //select * from login1 where username in (select username from login2)

      val conf = new SparkConf().setAppName("DataSet Application").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      val bc: Broadcast[Array[String]] = sc.broadcast(Array[String]("login3", "login4"))
      val x = Array(("login1", 192), ("login2", 146), ("login3", 72))
      val xdf = sqlContext.createDataFrame(x).toDF("name", "cnt")

      val func: (String => Boolean) = (arg: String) => bc.value.contains(arg)
      val sqlfunc = udf(func)
      val filtered = xdf.filter(sqlfunc(col("name")))

      xdf.show()
      filtered.show()

}
