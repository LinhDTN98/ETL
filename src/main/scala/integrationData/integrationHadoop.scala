package integrationData

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.functions.{current_date, lit, max, split}
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import java.net.URI

object integrationHadoop {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local[*]")
      .appName("Hive_Warehouse")
      .config("spark.sql.warehouse.dir", args(0))
      .config("spark.hadoop.hive.metastore.uris", args(1))
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .enableHiveSupport()
      .getOrCreate()


    val host_file_data = args(0)
    val folder_data = args(1)
    val comp = args(2)
    val schema = args(3)
    val tbl_name = args(4)
    val data_type = args(5)
    val url = args(6)
    val username = args(7)
    val password = args(8)
    val fields = args(9)
    val driver = args(10)


    val path = s"/$folder_data/$comp/$schema/$data_type/$tbl_name"

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(s"$host_file_data"), conf)

    //    var file_path = new org.apache.hadoop.fs.Path(path)

    val file_path = new Path(path)

    val exists = fs.exists(file_path)


    //    handle load data into datalake. Check file data exists if file exists then check data upsert.If data was upsert
    //    then insert new data under file parquet into datalake.


    var dataQuery = ""

    val runTime = split(current_date(), "-")
    val year = runTime(0)
    val month = runTime(1)
    val day = runTime(2)


    if (exists) {

      //    Handle take file latest in HDFS

      def listFilesRecursively(path: Path): Array[FileStatus] = {
        val files = fs.listStatus(path)

        files.flatMap {
          case file if file.isDirectory => listFilesRecursively(file.getPath)  // Recurse into subdirectories
          case file if file.isFile => Array(file)  // Collect files
        }
      }

      // Get all files under the root directory
      val allFiles = listFilesRecursively(file_path).filter(file => file.getPath.getName.endsWith(".parquet"))


      // Find the latest file based on modification time
      val latestFile = allFiles.maxBy(_.getModificationTime)


      //      Delta load into file in DataLake

      val tblLocation = s"${latestFile.getPath}"

      val df = spark.read.parquet(tblLocation)
      val date_max_df = df.agg(max("last_modified_date")).head()
      val date_max = date_max_df(0)

      //      dataQuery = s"(SELECT $fields FROM $tbl_name where comp_code_load = '$comp' and (create_date > TO_TIMESTAMP('$date_max','YYYY-MM-DD HH24:MI:SS')" +
      //        s" or modified_date > TO_TIMESTAMP('$date_max','YYYY-MM-DD HH24:MI:SS'))) as tmp"

      dataQuery = s"(SELECT $fields FROM $tbl_name where (created_date > TO_TIMESTAMP('$date_max','YYYY-MM-DD HH24:MI:SS')" +
        s" or last_modified_date > TO_TIMESTAMP('$date_max','YYYY-MM-DD HH24:MI:SS'))) as tmp"

    }

    else {

      //      dataQuery = s"(SELECT $fields FROM $tbl_sql where comp_code_load = '$comp') as tmp"
      dataQuery = s"(SELECT $fields FROM $tbl_name) as tmp"
    }

    try {

      val jdbcDF = spark.read.format("jdbc")
        .options(Map("url" -> url,
          "driver" -> driver,
          "dbtable" -> dataQuery,
          "user" -> username,
          "password" -> password,
          "header" -> "true",
          "fetchSize" -> "10000"))
        .load()

      //  If Success then integration to hdfs

      val tblHdfsPath = s"$host_file_data/$path"

      if (jdbcDF.count() > 0) {

        val outputDF = jdbcDF.withColumn("year", lit(year)).withColumn("month", lit(month))
          .withColumn("day", lit(day))

        outputDF.write.partitionBy("year", "month", "day").mode(SaveMode.Append).parquet(tblHdfsPath)
      }
    }

    catch {
      case e: AnalysisException =>
        println(s"Table '$tbl_name' not found in source system: ${e.getMessage}")
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
    }

    spark.stop()
  }
}
