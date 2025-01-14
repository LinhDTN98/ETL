package integrationData

import org.apache.spark.sql.SparkSession

class hiveRpt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master", "local[*]")
      .appName("Integration_HIVE_RPT_DATA_LAYER")
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


    val comp = args(2)
    val schema = args(3)
    val tbl_name = args(4)
    val create_stm = args(5)
    val fields  = args(6)


    // Hive rpt table in RPT_LAYER with prefix is rpt_<table_name>
    // Check source table in RAW_LAYER. If table not exists then create table. Script create table from read table
    // config. If table exists then check data with key to insert or update data in rpt_table

    spark.sql(s"create database if not exists $comp")

    val tblStgName = comp + ".stg_" + schema + "_" + tbl_name

    val check_table = spark.catalog.tableExists(s"$tblStgName")

    //      if file exists then append data into table raw hive

    val tblRptName = comp + ".rpt_" + schema + "_" + tbl_name

    if (check_table) {

      val tblHiveStg = spark.sql(s"select $fields from $tblStgName")

      tblHiveStg.write.mode("overwrite").saveAsTable(tblRptName)

    }

    else
    {
      val create_stmt = String.format(s"$create_stm", tblRptName)
      spark.sql(create_stmt)

    }
  }
}
