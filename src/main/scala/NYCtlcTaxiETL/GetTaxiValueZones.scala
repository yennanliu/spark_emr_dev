package NYCtlcTaxiETL

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetTaxiValueZones {
    private val logger = Logger.getLogger(this.getClass)

    def main(args: Array[String]) {

      // check arguments
      // if (args.length != 4)
      //   throw new IllegalArgumentException(
      //     "Parameters : "+
      //     "<yellowSource> <greenSource> <zonesSource> <targetBucket> "+
      //     "(multiple source paths can be provided in the same string, separated by a coma"
      //   )

      logger.setLevel(Level.INFO)
      lazy val session =
          SparkSession.builder
            .appName("GetTaxiValueZones")
            .config("spark.master", "local[*]")
            .getOrCreate()

      try {
        // runJob(sparkSession = session,
        //       yellow = args(0).split(",").toList,
        //       green = args(1).split(",").toList,
        //       zones = args(2).split(",").toList,
        //       target = args(3)
        //       )
        runJob(sparkSession = session )
        session.stop()
        } catch {
            case ex: Exception =>
              logger.error(ex.getMessage)
              logger.error(ex.getStackTrace.toString)
        }
    }

    def runJob(sparkSession :SparkSession) = {

        logger.info("Execution started")

        import sparkSession.implicits._

        sparkSession.conf.set("spark.sql.session.timeZone", "America/New_York")

        // all dataset 
        // var yellow_trip_data = "s3a://nyc-tlc-taxi/yellow_trip" 
        // var green_trip_data = "s3a://nyc-tlc-taxi/green_trip"
        // var fhv_trip_data = "s3a://nyc-tlc-taxi/fhv_trip"
        var zone_data = "s3a://nyc-tlc-taxi/zone/taxi+_zone_lookup.csv"

        // sample dataset
        var yellow_trip_data = "s3a://nyc-tlc-taxi/yellow_trip/dt=2014-01/*.csv" 
        var green_trip_data = "s3a://nyc-tlc-taxi/green_trip/dt=2014-01/*.csv"
        var fhv_trip_data = "s3a://nyc-tlc-taxi/fhv_trip/dt=2014-01/*.csv"
        var outout_data = "s3a://nyc-tlc-taxi/scala_etl_output/GetTaxiValueZones"

        val yellowEvents = sparkSession.read
          .option("header","true")
          .option("inferSchema", "true")
          .option("enforceSchema", "false")
          .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
          .option("columnNameOfCorruptRecord", "error")
          .csv(yellow_trip_data)  //.csv(yellow: _*)
          .filter(col("Trip_Pickup_DateTime").gt("2017"))
          .filter(col("Trip_Dropoff_DateTime").lt("2019"))
          .withColumn("duration", unix_timestamp($"Trip_Dropoff_DateTime").minus(unix_timestamp($"Trip_Pickup_DateTime")))
          .withColumn("minute_rate",$"Trip_Distance".divide($"duration") * 60)
          .withColumnRenamed("Trip_Pickup_DateTime","pickup_datetime")
          .select("pickup_datetime","minute_rate","Total_Amt") //.select("pickup_datetime","minute_rate","PULocationID","Total_Amt")
          .limit(100)
          //.withColumn("taxiColor",lit("yellow"))


        // val greenEvents = sparkSession.read
        //   .option("header","true")
        //   .option("inferSchema", "true")
        //   .option("enforceSchema", "false")
        //   .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
        //   .option("columnNameOfCorruptRecord", "error")
        //   .csv(green_trip_data)  //.csv(yellow: _*)
        //   .filter(col("Trip_Pickup_DateTime").gt("2017"))
        //   .filter(col("Trip_Dropoff_DateTime").lt("2019"))
        //   .withColumn("duration", unix_timestamp($"Trip_Dropoff_DateTime").minus(unix_timestamp($"Trip_Pickup_DateTime")))
        //   .withColumn("minute_rate",$"Trip_Distance".divide($"duration") * 60)
        //   .withColumnRenamed("Trip_Pickup_DateTime","pickup_datetime")
        //   .select("pickup_datetime","minute_rate","Total_Amt") //.select("pickup_datetime","minute_rate","PULocationID","Total_Amt")
        //   .limit(100)
          //.withColumn("taxiColor",lit("green"))

        // val zonesInfo = sparkSession.read
        //     .option("header","true")
        //     .option("inferSchema", "true")
        //     .option("enforceSchema", "false")
        //     .option("columnNameOfCorruptRecord", "error")
        //     .csv(zone_data) //.csv(zones: _*)

        // val allEventsWithZone = greenEvents
        //   .union(yellowEvents)
        //   .join(zonesInfo,$"PULocationID" === $"LocationID")
        //   .select("pickup_datetime","minute_rate","taxiColor","LocationID","Borough", "Zone")

        //allEventsWithZone.cache

        yellowEvents.show()
        //greenEvents.show()

        // val allEvents = greenEvents.union(yellowEvents)
        //       .select("pickup_datetime","minute_rate")

        var allEvents = yellowEvents
        allEvents.cache

        // val zoneAttractiveness = allEventsWithZone
        //   .groupBy($"LocationID", date_trunc("hour",$"pickup_datetime") as "pickup_hour")
        //   .pivot("taxiColor",Seq("yellow", "green"))
        //   .agg("minute_rate" -> "avg", "minute_rate" -> "count")
        //   .withColumnRenamed("yellow_avg(minute_rate)","yellow_avg_minute_rate")
        //   .withColumnRenamed("yellow_count(minute_rate)","yellow_count")
        //   .withColumnRenamed("green_avg(minute_rate)","green_avg_minute_rate")
        //   .withColumnRenamed("green_count(minute_rate)","green_count")

        print (">>>>>>>>>> SAVE OUTPUT  (parquet)")

        // allEventsWithZone.show()
        // zoneAttractiveness.show()

        allEvents.show()

        allEvents.repartition(1)
        .sortWithinPartitions($"pickup_hour")
        .write
        .mode("OVERWRITE")
        .partitionBy("pickup_datetime")
        .parquet(outout_data + "/allEvents")

        // val rawQuery = allEventsWithZone
        //   .withColumn("year", year($"pickup_datetime"))
        //   .withColumn("month", month($"pickup_datetime"))
        //   .withColumn("day", dayofmonth($"pickup_datetime"))
        //   .repartition($"year",$"month")
        //   .sortWithinPartitions("day")
        //   .write
        //   .mode("OVERWRITE")
        //   .partitionBy("year","month")
        //   .parquet(outout_data + "/allEventsWithZone")

        // val aggregateQuery = zoneAttractiveness
        //   .repartition(1)
        //   .sortWithinPartitions($"pickup_hour")
        //   .write
        //   .mode("OVERWRITE")
        //   .partitionBy("LocationID")
        //   .parquet(outout_data + "/zoneAttractiveness")
    }
  
}