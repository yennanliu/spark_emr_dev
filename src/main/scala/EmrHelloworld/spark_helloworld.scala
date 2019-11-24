package EmrHelloworld

import org.apache.spark._

object spark_helloworld {
  def main(args: Array[String]): Unit = {
    //setup spark
    //val sc = new SparkContext(new SparkConf())
    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    val rdd = sc.sparkContext.parallelize(Array(1 to 10))
    rdd.count()
    println("************")
    println("************")   
    // terminate spark context
    spark.stop()

  }
}