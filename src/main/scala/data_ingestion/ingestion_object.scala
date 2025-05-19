package data_ingestion

import config.config_obj
import fromurl.webapi_obj
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.log4j.Logger

object ingestion_object {

  val logger = Logger.getLogger(getClass.getName)

  def fetchFromApiAsDataFrame(spark: SparkSession): DataFrame = {

    logger.info("******** Starting data ingestion ********")

    val url = config_obj.api.endpoint
    val response = webapi_obj.fetchurl(url)

    logger.info(s"fetch data From Api  : $url")

    val rdd = spark.sparkContext.parallelize(Seq(response))
    val df = spark.read
    .option("multiline",true)
    .json(rdd)

    logger.info(s"data fetched ${df.count()} ")

    df

  }

  def readCSVFromS3(spark: SparkSession): DataFrame = {
    val fullPath = s"s3a://${config_obj.s3Bucket}/${config_obj.s3Path}"
    logger.info(s"Reading CSV from S3 path: $fullPath")

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("fs.s3a.access.key", config_obj.awsAccessKey)
      .option("fs.s3a.secret.key", config_obj.awsSecretKey)
      .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .option("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .load(fullPath)

    logger.info(s"Data read from S3 CSV. Record count: ${df.count()}")

    df
  }

  def setSnowflakeConfig(spark: SparkSession): Unit = {
    spark.conf.set("spark.datasource.snowflake.url", config_obj.snowflake.sfURL)
    spark.conf.set("spark.datasource.snowflake.user", config_obj.snowflake.sfUser)
    spark.conf.set("spark.datasource.snowflake.password", config_obj.snowflake.sfPassword)
    spark.conf.set("spark.datasource.snowflake.database", config_obj.snowflake.sfDatabase)
    spark.conf.set("spark.datasource.snowflake.schema", config_obj.snowflake.sfSchema)
    spark.conf.set("spark.datasource.snowflake.warehouse", config_obj.snowflake.sfWarehouse)
    spark.conf.set("spark.datasource.snowflake.role", config_obj.snowflake.sfRole)
  }

  def readFromSnowflake(spark: SparkSession, tableName: String): DataFrame = {
    logger.info(s"Reading from Snowflake table: $tableName")

    val df = spark.read
      .format("snowflake")
      .option("sfURL", spark.conf.get("spark.datasource.snowflake.url"))
      .option("sfUser", spark.conf.get("spark.datasource.snowflake.user"))
      .option("sfPassword", spark.conf.get("spark.datasource.snowflake.password"))
      .option("sfDatabase", spark.conf.get("spark.datasource.snowflake.database"))
      .option("sfSchema", spark.conf.get("spark.datasource.snowflake.schema"))
      .option("sfWarehouse", spark.conf.get("spark.datasource.snowflake.warehouse"))
      .option("sfRole", spark.conf.get("spark.datasource.snowflake.role"))
      .option("dbtable", tableName)
      .load()

    logger.info(s"Snowflake table read successful: ${df.count()} rows")
    df
  }

}
  
  
