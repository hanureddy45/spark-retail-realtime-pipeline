package main_pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import data_ingestion.ingestion_object
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

import utils.safejsonutils

object main_object {
  
  val logger = Logger.getLogger(getClass.getName)
  
  def main(args:Array[String]):Unit ={
    
    val conf = new SparkConf().setAppName("main")
                              .setMaster("local[*]")
     
    val sc = new SparkContext(conf) 
    sc.setLogLevel("ERROR")
    
   implicit val spark = SparkSession.builder().getOrCreate()
    
     // 👇 Load log4j.properties
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    
    logger.info(s"spark conf and spark context and spark session created")
   
    val rawdf = ingestion_object.fetchFromApiAsDataFrame(spark)
    
   
   rawdf.show()
   rawdf.printSchema()
    
     logger.info(s"data is converted as data frame")
     
    // df.printSchema()
     
   // val flattenedDf = jsonflatten.flattenWebApiJson(df)
    //flattenedDf.show(false)
    
   // flattenedDf.printSchema()
    
    val safejson = safejsonutils.safelyFlattenWebApiJson(rawdf)
    
    safejson.show()
    safejson.printSchema()
    
   val s3df = ingestion_object.readCSVFromS3(spark)
  s3df.show()
    
    // Now you can read from Snowflake
   
   
     // ✅ Step 1: Set Snowflake configs before reading
    ingestion_object.setSnowflakeConfig(spark)
   
     val snowflake_df = ingestion_object.readFromSnowflake(spark,"SALES_ORDERS")
   snowflake_df.show()
    
  }
  
  
}