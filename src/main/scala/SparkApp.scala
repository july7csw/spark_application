import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


object SparkApp extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  val metadataPath = "data/metadata"

  val spark = SparkSession.builder()
    .appName("spark-demo")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.metadata.dir", metadataPath)
    .master("local[*]")
    .getOrCreate()

  val hiveTablePath = "data/hivetable"
  val hiveTableName = "logtable"

  val logSchema = StructType(List(
    StructField("event_time", TimestampType),
    StructField("event_type", StringType),
    StructField("product_id", StringType),
    StructField("category_id", StringType),
    StructField("category_code", StringType),
    StructField("brand", StringType),
    StructField("price", FloatType),
    StructField("user_id", StringType),
    StructField("user_session", StringType)
  ))

  // 추가 기간 처리 대응을 위해 처리되지 않은 csv 파일만 처리
  val logFilePath = "data/log"
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val allFiles = fs.listStatus(new Path(logFilePath)).map(_.getPath.toString)
  val processedFiles = fs.listStatus(new Path(metadataPath)).map(_.getPath.toString)
  val unprocessedFiles = allFiles.filterNot(processedFiles.contains)
  val tempTableName = "temptable"

  unprocessedFiles.foreach { filePath =>
    val sampleDF = spark.read
      .schema(logSchema)
      .option("header", "true")
      .csv(filePath)
      .withColumn("event_time_KST", from_utc_timestamp(col("event_time"), "Asia/Seoul"))
      .withColumn("event_date_KST", col("event_time_KST").cast(DateType))

    sampleDF.createOrReplaceTempView(tempTableName)

    try {
      // KST 기준 일별 파티션, External table로 저장
      sampleDF
        .write
        .partitionBy("event_date_KST")
        .mode(SaveMode.Append)
        .option("path", hiveTablePath)
        .saveAsTable(hiveTableName)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        println("Failed to save table:" + e.getMessage)
    }
  }
  spark.stop()
}