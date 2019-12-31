package NYCtlcTaxiETL

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}


object GetTopPickupZone {

    def main(args: Array[String]) {

      lazy val sparksession = SparkSession.builder
              .appName("GetTopPickupZone")
              .config("spark.master", "local[*]")
              .getOrCreate()

        import sparksession.implicits._

        sparksession.conf.set("spark.sql.session.timeZone", "America/New_York")

        var zone_data = "s3a://nyc-tlc-taxi/zone/taxi+_zone_lookup.csv"

        // sample dataset
        var processed_yellow_trip_data = "s3a://nyc-tlc-taxi/scala_etl_output/PreProcessYellowTaxiTrip/trip_year=2009/trip_month=01/*.parquet" 
        var output_filename = "s3a://nyc-tlc-taxi/scala_etl_output/scala_etl_output/GetTopPickupZone" 


        val yellow_trip_data = sparksession.read.parquet(processed_yellow_trip_data)
        var yellow_trip_data_DF = yellow_trip_data.toDF()

        println(yellow_trip_data_DF.schema)

        // pickup_datetime -> pickup_date
        var yellow_trip_data_DF_ = yellow_trip_data_DF.withColumn("pickup_date", (col("pickup_datetime").cast("date"))) 
        var pickup_location_id_trip_distance = yellow_trip_data_DF_.groupBy("pickup_date","pickup_location_id", "trip_distance").count()
        pickup_location_id_trip_distance.show()

        print (">>>>>>>>>> SAVE OUTPUT  (csv)")
        pickup_location_id_trip_distance.write
                                        .format("csv")
                                        .mode("OVERWRITE")
                                        .partitionBy("pickup_date")
                                        .save(output_filename)
     
                        } 
        }