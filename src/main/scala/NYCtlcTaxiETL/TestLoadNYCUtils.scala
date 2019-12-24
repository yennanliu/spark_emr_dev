package NYCtlcTaxiETL

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar

import NYCTaxiUtils.TripSchemaUtils

object TestLoadNYCUtils {
  def main(args: Array[String]): Unit = {

    println (">>> Run func TestLoadNYCUtils")
    
    TripSchemaUtils

    var test_schema1 = TripSchemaUtils.getYellowTaxiSchema(2016, 1)
    var test_schema2 = TripSchemaUtils.getGreenTaxiSchema(2016, 1)

    println ("test_schema1")
    println (test_schema1)
    
    println ("test_schema2")
    println (test_schema2)

  }
}