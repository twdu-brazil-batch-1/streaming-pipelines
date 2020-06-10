package com.tw.apps

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{udf}

object StationUtils {

  implicit class StringDataset(val stations: Dataset[StationData]) {
    def formatLastUpdatedDate(dateFormat: String, spark: SparkSession) = {
      import spark.implicits._
      val udfToDateUTC = udf((epochSeconds: Long) => {
        val dateFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(java.time.ZoneId.of("UTC"))
        dateFormatter.format(java.time.Instant.ofEpochSecond(epochSeconds))
      })
      stations.withColumn("last_updated", udfToDateUTC($"last_updated"))
    }
  }

}
