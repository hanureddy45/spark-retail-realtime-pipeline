package utils

import org.apache.spark.sql.{Column,DataFrame,SparkSession}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import javax.xml.validation.Schema
import org.apache.spark.sql.types.StructType

object safejsonutils {
  
  val logger = Logger.getLogger(getClass.getName)
   logger.info(" checking schema is present or not ")
   
  
 def columnExists(df: DataFrame, colName: String): Boolean = {
  try {
    val parts = colName.split("\\.")
    var currentSchema = df.schema

    for (i <- parts.indices) {
      val fieldName = parts(i)
      val field = currentSchema.find(_.name == fieldName)
      if (field.isEmpty) return false

      field.get.dataType match {
        case struct: StructType => currentSchema = struct
        case _ =>
          if (i != parts.length - 1) return false
      }
    }

    true
  } catch {
    case _: Throwable => false
  }
}
  
def safeCol(colPath: String, df: DataFrame): Column = {
  if (columnExists(df, colPath)) {
    col(colPath)
  } else {
    logger.warn(s"⚠️ Column missing: $colPath")
    lit(null).as(colPath.split("\\.").last)
  }
}
  // ✅ 3. Flattening function with correct DataFrame usage
  def safelyFlattenWebApiJson(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("🔁 Flattening Web API JSON dynamically...")

    if (!df.columns.contains("results")) {
      logger.error("❌ 'results' field missing. Cannot flatten.")
      return df.withColumn("flatten_error", lit("Missing results"))
    }

    // ✅ Use explodedDf for safeCol calls for nested fields
    val explodedDf = df.withColumn("results", explode(col("results")))

   explodedDf.select(
  safeCol("info.page", explodedDf),
  safeCol("info.results", explodedDf),
  safeCol("info.seed", explodedDf),
  safeCol("info.version", explodedDf),
  safeCol("results.cell", explodedDf),
  safeCol("results.dob.age", explodedDf),
  safeCol("results.dob.date", explodedDf),
  safeCol("results.email", explodedDf),
  safeCol("results.gender", explodedDf),
  safeCol("results.id.name", explodedDf),
  safeCol("results.id.value", explodedDf),
  safeCol("results.location.city", explodedDf),
  safeCol("results.location.coordinates.latitude", explodedDf),
  safeCol("results.location.coordinates.longitude", explodedDf),
  safeCol("results.location.country", explodedDf),
  safeCol("results.location.postcode", explodedDf),
  safeCol("results.location.state", explodedDf),
  safeCol("results.location.street.number", explodedDf),
  safeCol("results.location.street.name", explodedDf),
  safeCol("results.location.timezone.description", explodedDf),
  safeCol("results.location.timezone.offset", explodedDf),
  safeCol("results.login.md5", explodedDf),
  safeCol("results.login.password", explodedDf),
  safeCol("results.login.salt", explodedDf),
  safeCol("results.login.sha1", explodedDf),
  safeCol("results.login.sha256", explodedDf),
  safeCol("results.login.username", explodedDf),
  safeCol("results.login.uuid", explodedDf),
  safeCol("results.name.first", explodedDf),
  safeCol("results.name.last", explodedDf),
  safeCol("results.name.title", explodedDf),
  safeCol("results.nat", explodedDf),
  safeCol("results.picture.large", explodedDf),
  safeCol("results.picture.medium", explodedDf),
  safeCol("results.picture.thumbnail", explodedDf),
  safeCol("results.registered.age", explodedDf),
  safeCol("results.registered.date", explodedDf)
)
  }
  
 }