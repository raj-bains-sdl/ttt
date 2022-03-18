package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Customer {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "dev" =>
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",".stripPrefix("\\"))
          .schema(
            StructType(
              Array(
                StructField("c_customer_sk",          IntegerType, true),
                StructField("c_customer_id",          StringType,  true),
                StructField("c_current_cdemo_sk",     IntegerType, true),
                StructField("c_current_hdemo_sk",     IntegerType, true),
                StructField("c_current_addr_sk",      IntegerType, true),
                StructField("c_first_shipto_date_sk", IntegerType, true),
                StructField("c_first_sales_date_sk",  IntegerType, true),
                StructField("c_salutation",           StringType,  true),
                StructField("c_first_name",           StringType,  true),
                StructField("c_last_name",            StringType,  true),
                StructField("c_preferred_cust_flag",  StringType,  true),
                StructField("c_birth_day",            IntegerType, true),
                StructField("c_birth_month",          IntegerType, true),
                StructField("c_birth_year",           IntegerType, true),
                StructField("c_birth_country",        StringType,  true),
                StructField("c_login",                StringType,  true),
                StructField("c_email_address",        StringType,  true),
                StructField("c_last_review_date",     IntegerType, true)
              )
            )
          )
          .load("dbfs:/data/csv_sources/customer_csv/")
          .cache()
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
