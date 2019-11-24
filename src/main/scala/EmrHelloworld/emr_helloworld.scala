package EmrHelloworld

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

object emr_helloworld {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setAppName("Spark emr Hello world")

    val sc = new SparkContext(conf)
    //your algorithm
    val n = 10000000
    val count = sc.parallelize(1 to n).map { i =>
      val x = scala.math.random
      val y = scala.math.random
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
  }
}