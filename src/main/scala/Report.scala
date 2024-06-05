package com.dodat.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Report {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("job02")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df = spark.read
      .parquet("data\\20240602_20240602_crash_android_streaming_000000000000.parquet",
               "data\\20240602_20240602_crash_ios_streaming_000000000000.parquet")
    val crash_total = df.agg(count("*")).first().getLong(0)

    // Crash DF
    val crash_df = df.groupBy("error_type")
      .agg(count("event_id").alias("Number of crashes"),
        countDistinct("installation_uuid").alias("Crashed Users"),
        countDistinct("device").alias("Crashed Devices"),
        countDistinct("issue_title", "issue_subtitle").alias("Number of issues"))
      .withColumn("Crash Rate (%)", col("Number of crashes") * 100 / crash_total)
    crash_df.show(truncate = false)

    // App Version DF
    val app_version_df = df.groupBy("application")
      .agg(count("event_id").alias("Crashes"), countDistinct("installation_uuid").alias("Users"))
      .withColumn("Version", col("application").getField("display_version"))
      .withColumn("Error Rate (%)", col("Crashes") * 100 / crash_total)
      .select("Version", "Crashes", "Users", "Error Rate (%)")
      .orderBy(desc("Error Rate (%)"))
    app_version_df.show(truncate = false)

    // Device Version DF
    val device_version_df = df.groupBy("device")
      .agg(count("event_id").alias("Crashes"), countDistinct("installation_uuid").alias("Users"))
      .withColumn("Device", col("device").getField("model"))
      .withColumn("Error Rate (%)", col("Crashes") * 100 / crash_total)
      .select("Device", "Crashes", "Users", "Error Rate (%)")
      .orderBy(desc("Error Rate (%)"))
    device_version_df.show(truncate = false)

    // OS Versions DF
    val os_version_df = df.groupBy("operating_system")
      .agg(count("event_id").alias("Crashes"), countDistinct("installation_uuid").alias("Users"))
      .withColumn("OS Version", col("operating_system").getField("display_version"))
      .withColumn("Error Rate (%)", col("Crashes") * 100 / crash_total)
      .select("OS Version", "Crashes", "Users", "Error Rate (%)")
      .orderBy(desc("Error Rate (%)"))
    os_version_df.show(truncate = false)

    // Issue DF
    val issue_df = df.groupBy("issue_title", "issue_subtitle", "application")
      .agg(count("event_id").alias("Crashes"), countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("issue_title", "Issue")
      .withColumnRenamed("issue_subtitle", "Blame Frame")
      .withColumn("Version", col("application").getField("display_version"))
      .select("Issue", "Blame Frame", "Version", "Crashes", "Users")
      .orderBy(desc("Crashes"))
    issue_df.show(truncate = false)
  }
}