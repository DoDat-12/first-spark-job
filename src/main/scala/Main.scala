package com.dodat.scala

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.col

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("job01")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // android
    val df1 = spark.read.parquet("data\\20240602_20240602_crash_android_streaming_000000000000.parquet")
    // ios
    val df2 = spark.read.parquet("data\\20240602_20240602_crash_ios_streaming_000000000000.parquet")

    df1.printSchema()

    df1.createOrReplaceTempView("android")
    df2.createOrReplaceTempView("ios")

// Thống kê số lượng lỗi crash FATAL và ANR trên các ứng dụng, version ứng dụng
    val result1 = spark.sql(
      """
         select project_name, error_type, count(error_type) as number_of_errors
            from android
            where error_type in ("FATAL", "ANR")
            group by project_name, error_type
         union
         select project_name, error_type, count(error_type) as number_of_errors
            from ios
            where error_type in ("FATAL", "ANR")
            group by project_name, error_type
         order by project_name ASC;
      """
    )
    result1.show(20, truncate = false)

// Số lượng người dùng gặp hiện tượng Crash FATAL và ANR trên các ứng dụng, version ứng dụng
    val result2 = spark.sql(
      """
         select project_name, error_type, count(distinct installation_uuid) as number_of_customers
            from android
            where error_type in ("FATAL", "ANR")
            group by project_name, error_type
         union
         select distinct project_name, error_type, count(distinct installation_uuid) as number_of_customers
            from ios
            where error_type in ("FATAL", "ANR")
            group by project_name, error_type
         order by project_name ASC;
      """
    )
    result2.show(20, truncate = false)

// Thống kê tỉ lệ lỗi theo các version OS thiết bị người dùng trên các ứng dụng, version ứng dụng
//    Nếu không phân biệt loại thiết bị
//    val tmp_df = df1.withColumn("operating_system", col("operating_system").getField("display_version"))
//    tmp_df.createOrReplaceTempView("android_tmp")

    val result3 = spark.sql(
      """
         select t1.project_name as project_name, t1.operating_system as os_version, (count(t1.operating_system) * 100 / app_total_errors.total_errors) as error_rates
         from android t1
         join (
            select project_name, count(*) as total_errors
            from android
            group by project_name -- count number of errors in each project_name
         ) as app_total_errors
         on t1.project_name = app_total_errors.project_name
         group by t1.project_name, t1.operating_system, app_total_errors.total_errors
         union
         select t2.project_name as project_name, t2.operating_system as os_version, (count(t2.operating_system) * 100 / app_total_errors.total_errors) as error_rates
         from ios t2
         join (
            select project_name, count(*) as total_errors
            from ios
            group by project_name -- count number of errors in each project_name
         ) as app_total_errors
         on t2.project_name = app_total_errors.project_name
         group by t2.project_name, t2.operating_system, app_total_errors.total_errors
         order by project_name ASC, error_rates DESC;
      """
    )
    result3.show(50, truncate = false)

// Thống kê số lượng lỗi và số lượng người dùng gặp lỗi trên các version os, tên thiết bị của client, version của app
    val result4 = spark.sql(
      """
         select operating_system as os_version, device, application as app_version, count(*) as number_of_errors, count(distinct installation_uuid) as number_of_customers
         from android
         group by operating_system, device, application
         union
         select operating_system as os_version, device, application as app_version, count(*) as number_of_errors, count(distinct installation_uuid) as number_of_customers
         from ios
         group by operating_system, device, application
         order by number_of_customers DESC
      """
    )
    result4.show(truncate = false)


// Thông kê nguyên nhân gây lỗi tương ứng với version ứng dụng, số lượng crash, số lượng user gặp hiện tượng crash trên các ứng dụng, version ứng dụng
    val result5 = spark.sql(
      """
         select application as app_version, issue_title, issue_subtitle, count(*) as number_of_errors, count(distinct installation_uuid) as number_of_customers
         from android
         group by application, issue_title, issue_subtitle
         union
         select application as app_version, issue_title, issue_subtitle, count(*) as number_of_errors, count(distinct installation_uuid) as number_of_customers
         from ios
         group by application, issue_title, issue_subtitle
         order by number_of_customers DESC
      """
    )
    result5.show(truncate = false)
  }
}