package EmrHelloworld

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object spark_helloworld {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setAppName("SparkHelloWorld")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    val rdd = spark.sparkContext.parallelize(Array(1 to 10))
    rdd.count()
    println("************")
    println("************")   
    // terminate spark context
    //spark.stop()

  }
}