package com.github.fescalhao

import com.github.SparkPackageUtils
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration


object DeltaLakeQuickStart extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkPackageUtils.getSparkSession("Delta Lake Quick Start")

    writeStreaming(spark)
  }

  def firstExample(spark: SparkSession): Unit = {
    val data = spark.range(11, 21)

    data
      .withColumn("mod", col("id").mod(2))
      .repartition(1)
      .orderBy("mod", "id")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("mod")
      .format("delta")
      .save("./tables/delta-table")

    val df = spark
      .read
      .format("delta")
      .load("./tables/delta-table")

    df.show()

    val deltaTable = DeltaTable.forPath("./tables/delta-table")
    deltaTableMergeExample(spark, deltaTable)

    deltaTable.toDF.show()

    val dfOld = spark
      .read
      .format("delta")
      .option("versionAsOf", 3)
      .load("./tables/delta-table")

    dfOld.show()
  }

  def deltaTableMergeExample(spark: SparkSession, deltaTable: DeltaTable): Unit = {
    deltaTable.update(
      col("mod") === 0,
      Map("id" -> expr("id + 100"))
    )

    deltaTable.delete(col("id") < 15)

    val newData = spark
      .range(116, 121)
      .withColumn("mod", col("id").mod(2))

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        col("oldData.id") === col("newData.id")
      )
      .whenMatched()
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched()
      .insert(Map("id" -> col("newData.id"), "mod" -> col("newData.mod")))
      .execute()
  }

  def writeStreaming(spark: SparkSession): Unit = {
    val streamingDF = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    streamingDF
      .select(col("value").alias("id"), col("timestamp"))
      .repartition(1)
      .writeStream
      .format("delta")
      .trigger(Trigger.ProcessingTime(Duration(1, TimeUnit.MINUTES)))
      .outputMode(OutputMode.Append())
      .option("queryName", "writeStreamingTest")
      .option("checkpointLocation", "./tables/checkpoint")
      .option("maxBytesPerTrigger", 50)
      .start("./tables/delta-table-stream")
      .awaitTermination()
  }

  def readStreaming(spark: SparkSession): Unit = {
    spark
      .readStream
      .format("delta")
      .load("./tables/delta-table-stream")
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}
