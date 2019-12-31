package NYCtlcTaxiETL

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}


object GetPickupZoneStats {

    def main(args: Array[String]) {

      lazy val sparksession = SparkSession.builder
              .appName("GetPickupZoneStats")
              .config("spark.master", "local[*]")
              .getOrCreate()

        import sparksession.implicits._

        sparksession.conf.set("spark.sql.session.timeZone", "America/New_York")

        var zone_data = "s3a://nyc-tlc-taxi/zone/taxi+_zone_lookup.csv"

        // sample dataset
        //var processed_yellow_trip_data = "s3a://nyc-tlc-taxi/scala_etl_output/PreProcessYellowTaxiTrip/trip_year=2009/trip_month=01/*.parquet" 
        var processed_yellow_trip_data = "s3a://nyc-tlc-taxi/scala_etl_output/PreProcessYellowTaxiTrip/trip_year=2009/*/*.parquet" 

        var output_filename = "s3a://nyc-tlc-taxi/scala_etl_output/scala_etl_output/GetPickupZoneStats" 

        val yellow_trip_data = sparksession.read.parquet(processed_yellow_trip_data)
        var yellow_trip_data_DF = yellow_trip_data.toDF()

        println(yellow_trip_data_DF.schema)

        // UDF 
        val addOneUdf = udf { x: Long => x + 1 }
        val squared = udf((s: Long) => s * s)
        val divided = udf((x: Long, y: Long) => x / y )
        sparksession.udf.register("addOneUdf", addOneUdf)
        sparksession.udf.register("squared", squared)
        sparksession.udf.register("divided", divided)

        // pickup_datetime -> pickup_date
        // dropoff_datetime -> dropoff_date
        // trip_duration_sec 
        var yellow_trip_data_DF_ = yellow_trip_data_DF.withColumn("pickup_date", (col("pickup_datetime").cast("date")))
                                               .withColumn("dropoff_date", (col("dropoff_datetime").cast("date")))
                                               .withColumn("trip_duration_sec", col("dropoff_datetime").cast(LongType) - col("pickup_datetime").cast(LongType))
                                               // .withColumn("trip_duration_sec", col("trip_duration_sec").cast("double"))
                                               // .withColumn("trip_distance", col("trip_distance").cast("double"))
                                               .withColumn("avg_speed", divided($"trip_distance", $"trip_duration_sec")) //TODO : fix avg_speed value 
                                               //.withColumn("avg_speed", col("trip_distance").divide("trip_duration_sec"))
                                                                     
        var resultdf = yellow_trip_data_DF_.select("pickup_date","pickup_location_id", "trip_distance", "avg_speed")
        resultdf.show()

        print (">>>>>>>>>> SAVE OUTPUT  (csv)")
        resultdf.write
                .format("csv")
                .mode("OVERWRITE")
                .partitionBy("pickup_date")
                .save(output_filename)

                        } 
        }