package config

import com.typesafe.config.{Config,ConfigFactory}
import org.apache.spark.annotation.Private



object config_obj {
  
  
 private val config = ConfigFactory.load()

  val awsAccessKey: String = config.getString("aws.accessKey")
  val awsSecretKey: String = config.getString("aws.secretKey")
  val s3Bucket: String = config.getString("aws.s3Bucket")
  val s3Path: String = config.getString("aws.s3Path")
  val awsRegion: String = config.getString("aws.region")

  // SAFE: Reference local values, not config_obj.*
  println(s"AWS KEY from conf: $awsAccessKey")

  object api {
    val endpoint = config.getString("api.endpoint")
  }
 
 
 object snowflake {
    val sfURL = config.getString("snowflake.sfURL")
    val sfUser = config.getString("snowflake.sfUser")
    val sfPassword = config.getString("snowflake.sfPassword")
    val sfDatabase = config.getString("snowflake.sfDatabase")
    val sfSchema = config.getString("snowflake.sfSchema")
    val sfWarehouse = config.getString("snowflake.sfWarehouse")
    val sfRole = config.getString("snowflake.sfRole")
  }
 
  
}