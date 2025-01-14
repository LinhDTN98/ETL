package integrationData

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

import java.net.URI

object hiveStg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master", "local[*]")
      .appName("Integration_HIVE_RAW_DATA_LAYER")
      .config("spark.sql.warehouse.dir", args(0))
      .config("spark.hadoop.hive.metastore.uris", args(1))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .enableHiveSupport()
      .getOrCreate()

    val host_file_data = args(2) // host file HDFS
    val folder_data = args(3) // folder save file in datalake
    val comp = args(4)
    val schema = args(5)
    val tbl_name = args(6)
    val data_type = args(7)
    val listFieldKey = args(8)


    // Hive raw data with prefix is raw_<table_name>
    // 1.Check file table in datalake exists
    // 2.read file save into table


    val arr_listFieldKey = listFieldKey.split(",")

    val pathHadoop = s"/$folder_data/$comp/$schema/$data_type/$tbl_name"

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(s"$host_file_data"), conf)
    val file_path = new Path(pathHadoop)

    def listFilesRecursively(path: Path): Array[FileStatus] = {
      val files = fs.listStatus(path)

      files.flatMap {
        case file if file.isDirectory => listFilesRecursively(file.getPath) // Recurse into subdirectories
        case file if file.isFile => Array(file) // Collect files in hdfs repository
      }
    }


    try {

      // Get all files under the root directory
      val allFiles = listFilesRecursively(file_path).filter(file => file.getPath.getName.endsWith(".parquet"))

      //      if file exists then append data into table raw hive

      if (allFiles.length > 0) {
        // Find the latest file based on modification time
        val latestFile = allFiles.maxBy(_.getModificationTime)

        //      Delta load into file in DataLake

        val tblLocation = s"${latestFile.getPath}"

        val df = spark.read.parquet(tblLocation)

        spark.sql(s"create database if not exists $comp")

        val pathStgTbl = comp + ".stg_" + schema + "_" + tbl_name

        val check_table = spark.catalog.tableExists(pathStgTbl)


        if (check_table) {

          val maxTimeRawHive = spark.sql(s"select max(last_modified_date) from $pathStgTbl limit 1").head()

          val dataLoadTable = df.filter(s"last_modified_date > to_timestamp('${maxTimeRawHive(0)}')")

          if (dataLoadTable.count() > 0) {

            dataLoadTable.write.format("delta").mode("overwrite").saveAsTable(s"$comp.temp_$tbl_name")

            val listFieldInsert = df.columns

            // Generate the join condition dynamically

            val listFieldUpdate = listFieldInsert.filterNot(arr_listFieldKey.contains)
            val mapKey = arr_listFieldKey.map(col => s"a.${col.trim} = b.${col.trim}").mkString(" AND ")
            val listInsert = listFieldInsert.map(col => s"b.${col.trim}").mkString(",")
            val mapUpdate = listFieldUpdate.map(col => s"a.${col.trim} = b.${col.trim}").mkString(" , ")

            spark.sql(s"MERGE INTO $pathStgTbl as a " +
              s"USING $comp.temp_$tbl_name as b ON $mapKey " +
              s"WHEN MATCHED THEN " +
              s"UPDATE SET $mapUpdate " +
              s"WHEN NOT MATCHED THEN " +
              s"INSERT (${listFieldInsert.mkString(",")}) VALUES ($listInsert)")

            spark.sql(s"drop table $comp.temp_$tbl_name")

          }
        }

        else {

          val tbl_raw = spark.read.parquet(s"$host_file_data/$pathHadoop")
            .drop("year", "month", "day")

          // Define a window by key, ordering by modified_date and created_date

          val windowSpec = Window.partitionBy(arr_listFieldKey.toList.map(col):_*).orderBy(col("created_date").desc, col("last_modified_date").desc)

          // Add a rank to identify the latest record

          val rankedDF = tbl_raw.withColumn("rank", row_number().over(windowSpec))

          // Write Data into DataLake

          val resultRaw = rankedDF.filter("rank == 1").drop("rank")

          resultRaw.write.format("delta").mode("overwrite").saveAsTable(pathStgTbl)

        }
      }
    }
    catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
    }

    spark.stop()
  }
}
