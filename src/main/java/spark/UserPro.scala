package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object UserPro {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Test User and Product")
      .enableHiveSupport()
      .getOrCreate()

    println("load data from hive...")
    val priors = spark.sql("select * from badou.priors")
    val orders = spark.sql("select * from badou.orders")
    val products = spark.sql("select * from badou.products")

    val userOrd = orders.groupBy("user_id").agg(count("user_id") as "userOrd_cnt")

    userOrd.show(20,false)
  }
}