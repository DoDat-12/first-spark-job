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

    //-----------------------------------------------------DAY----------------------------------------------------------
    // Crash DF
    val crash_day_df = df.withColumn("Date", to_date(col("event_timestamp")))
      .groupBy("Date", "project_name", "error_type")
      .agg(count("event_id").alias("Number of crashes"),
        countDistinct("installation_uuid").alias("Crashed Users"),
        countDistinct("device").alias("Crashed Devices"),
        countDistinct("issue_title", "issue_subtitle").alias("Number of issues"))
      .withColumnRenamed("project_name", "Application")
      .withColumnRenamed("error_type", "Crash Type")
      .orderBy(asc("Date"), asc("Application"), asc("Crash Type"))
    crash_day_df.show(truncate = false)

    // App Version DF
    val app_version_day_df = df.withColumn("Date", to_date(col("event_timestamp")))
      .withColumn("Version", col("application").getField("display_version"))
      .groupBy("Date", "project_name", "Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Date"), asc("Application"), desc("Crashes"))
    app_version_day_df.show(truncate = false)

    // Device Version DF
    val device_version_day_df = df.withColumn("Date", to_date(col("event_timestamp")))
      .withColumn("Device", col("device").getField("model"))
      .groupBy("Date", "project_name", "Device")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Date"), asc("Application"), desc("Crashes"))
    device_version_day_df.show(truncate = false)

    // OS Versions DF
    val os_version_day_df = df.withColumn("Date", to_date(col("event_timestamp")))
      .withColumn("OS Version", col("operating_system").getField("display_version"))
      .groupBy("Date", "project_name", "OS Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Date"), asc("Application"), desc("Crashes"))
    os_version_day_df.show(truncate = false)

    // Issue DF
    val issue_day_df = df.withColumn("Date", to_date(col("event_timestamp")))
      .withColumn("Version", col("application").getField("display_version"))
      .groupBy("Date", "project_name", "issue_title", "issue_subtitle", "Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("issue_title", "Issue")
      .withColumnRenamed("issue_subtitle", "Blame Frame")
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Date"), asc("Application"), desc("Crashes"))
    issue_day_df.show(truncate = false)

    //-----------------------------------------------------WEEK---------------------------------------------------------
    // Crash DF
    val crash_week_df = df.withColumn("Week", weekofyear(col("event_timestamp")))
      .groupBy("Week", "project_name", "error_type")
      .agg(count("event_id").alias("Number of crashes"),
        countDistinct("installation_uuid").alias("Crashed Users"),
        countDistinct("device").alias("Crashed Devices"),
        countDistinct("issue_title", "issue_subtitle").alias("Number of issues"))
      .withColumnRenamed("project_name", "Application")
      .withColumnRenamed("error_type", "Crash Type")
      .orderBy(asc("Week"), asc("Application"), asc("Crash Type"))
    crash_week_df.show(truncate = false)

    // App Version DF
    val app_version_week_df = df.withColumn("Week", weekofyear(col("event_timestamp")))
      .withColumn("Version", col("application").getField("display_version"))
      .groupBy("Week", "project_name", "Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Week"), asc("Application"), desc("Crashes"))
    app_version_week_df.show(truncate = false)

    // Device Version DF
    val device_version_week_df = df.withColumn("Week", weekofyear(col("event_timestamp")))
      .withColumn("Device", col("device").getField("model"))
      .groupBy("Week", "project_name", "Device")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Week"), asc("Application"), desc("Crashes"))
    device_version_week_df.show(truncate = false)

    // OS Versions DF
    val os_version_week_df = df.withColumn("Week", weekofyear(col("event_timestamp")))
      .withColumn("OS Version", col("operating_system").getField("display_version"))
      .groupBy("Week", "project_name", "OS Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Week"), asc("Application"), desc("Crashes"))
    os_version_week_df.show(truncate = false)

    // Issue DF
    val issue_week_df = df.withColumn("Week", weekofyear(col("event_timestamp")))
      .withColumn("Version", col("application").getField("display_version"))
      .groupBy("Week", "project_name", "issue_title", "issue_subtitle", "Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("issue_title", "Issue")
      .withColumnRenamed("issue_subtitle", "Blame Frame")
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Week"), asc("Application"), desc("Crashes"))
    issue_week_df.show(truncate = false)

    //----------------------------------------------------MONTH---------------------------------------------------------
    // Crash DF
    val crash_month_df = df.withColumn("Month", date_format(to_date(col("event_timestamp")), "MM"))
      .groupBy("Month", "project_name", "error_type")
      .agg(count("event_id").alias("Number of crashes"),
        countDistinct("installation_uuid").alias("Crashed Users"),
        countDistinct("device").alias("Crashed Devices"),
        countDistinct("issue_title", "issue_subtitle").alias("Number of issues"))
      .withColumnRenamed("project_name", "Application")
      .withColumnRenamed("error_type", "Crash Type")
      .orderBy(asc("Month"), asc("Application"), asc("Crash Type"))
    crash_month_df.show(truncate = false)

    // App Version DF
    val app_version_month_df = df.withColumn("Month", date_format(to_date(col("event_timestamp")), "MM"))
      .withColumn("Version", col("application").getField("display_version"))
      .groupBy("Month", "project_name", "Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Month"), asc("Application"), desc("Crashes"))
    app_version_month_df.show(truncate = false)

    // Device Version DF
    val device_version_month_df = df.withColumn("Month", date_format(to_date(col("event_timestamp")), "MM"))
      .withColumn("Device", col("device").getField("model"))
      .groupBy("Month", "project_name", "Device")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Month"), asc("Application"), desc("Crashes"))
    device_version_month_df.show(truncate = false)

    // OS Versions DF
    val os_version_month_df = df.withColumn("Month", date_format(to_date(col("event_timestamp")), "MM"))
      .withColumn("OS Version", col("operating_system").getField("display_version"))
      .groupBy("Month", "project_name", "OS Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Month"), asc("Application"), desc("Crashes"))
    os_version_month_df.show(truncate = false)

    // Issue DF
    val issue_month_df = df.withColumn("Month", date_format(to_date(col("event_timestamp")), "MM"))
      .withColumn("Version", col("application").getField("display_version"))
      .groupBy("Month", "project_name", "issue_title", "issue_subtitle", "Version")
      .agg(count("event_id").alias("Crashes"),
        countDistinct("installation_uuid").alias("Users"))
      .withColumnRenamed("issue_title", "Issue")
      .withColumnRenamed("issue_subtitle", "Blame Frame")
      .withColumnRenamed("project_name", "Application")
      .orderBy(asc("Month"), asc("Application"), desc("Crashes"))
    issue_month_df.show(truncate = false)
  }
}