package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object OutCustomer {

  def apply(spark: SparkSession, in: DataFrame): Unit = {
    Config.fabricName match {
      case "dev" =>
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("dbfs:/tmp/ttt/customer")
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
