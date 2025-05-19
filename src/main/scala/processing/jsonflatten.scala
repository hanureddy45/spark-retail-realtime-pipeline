package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object jsonflatten {
  
  val logger = Logger.getLogger(getClass.getName)
  
   def flattenWebApiJson(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    logger.info("  start to flatten the json schema")
    
    val flatDF = df
      .withColumn("results", explode(col("results")))
      .select(
        col("info.page"),
        col("info.results"),
        col("info.seed"),
        col("info.version"),
        col("results.cell"),
        col("results.dob.age"),
        col("results.dob.date"),
        col("results.email"),
        col("results.gender"),
        col("results.id.name"),
        col("results.id.value"),
        col("results.location.city"),
        col("results.location.coordinates.latitude"),
        col("results.location.coordinates.longitude"),
        col("results.location.country"),
        col("results.location.postcode"),
        col("results.location.state"),
        col("results.location.street.number"),
        col("results.location.street.name"),
        col("results.location.timezone.description"),
        col("results.location.timezone.offset"),
        col("results.login.md5"),
        col("results.login.password"),
        col("results.login.salt"),
        col("results.login.sha1"),
        col("results.login.sha256"),
        col("results.login.username"),
        col("results.login.uuid"),
        col("results.name.first"),
        col("results.name.last"),
        col("results.name.title"),
        col("results.nat"),
        col("results.picture.large"),
        col("results.picture.medium"),
        col("results.picture.thumbnail"),
        col("results.registered.age"),
        col("results.registered.date")
      )
  logger.info("   flatten of json done ")
   
  flatDF
    
   
  }
  
  
}