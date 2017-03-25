import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by venkat on 25/03/17.
  *
  * Goal of this class is to understand how to use DF and Spark Sql
  */


object SparkSqlPlay {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Sql Play").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlJob = new SparkSqlJob(sc)
    sqlJob.run()
  }
}

//move this class outside SparkSqlJob due to outer class reference spark else it will attempt to serialize the outer class
case class Account(id: Long, clientName: String, country: String, balance: Long, currencyId: Long)
case class Currency(currencyId: Long, currency: String)

class SparkSqlJob(sc: SparkContext) {

  def joins(accountsStrRDD: RDD[String], currenciesStrRDD: RDD[String], sqlc: SQLContext): Unit = {
    val accountsDF = sqlc.createDataFrame(accountsStrRDD.map(s => s.split(","))
      .map(r => Account(r(0).trim.toLong, r(1).trim, r(2).trim, r(3).trim.toLong, r(4).trim.toLong)))
    val currenciesDF = sqlc.createDataFrame(currenciesStrRDD.map(s => s.split(","))
      .map(r => Currency(r(0).trim.toLong, r(1).trim)))
    accountsDF.join(currenciesDF, Seq("currencyId"), "leftouter").show()
  }


  def run(): Unit = {
    val accountsStrRDD = sc.textFile("./accounts.csv")
    val currenciesStrRDD = sc.textFile("./currencies.csv")
    val sqlc = new SQLContext(sc)

    style1(accountsStrRDD, sqlc)
    style2(accountsStrRDD, sqlc)
    joins(accountsStrRDD, currenciesStrRDD, sqlc)
  }

  def style2(accountsStrRDD: RDD[String], sqlc: SQLContext): Unit = {
    val accountsArrayRDD: RDD[Array[String]] = accountsStrRDD.map(s => s.split(","))
    val accountsRDD = accountsArrayRDD.map(r => Account(r(0).trim.toLong, r(1).trim, r(2).trim, r(3).trim.toLong, r(4).trim.toLong))
    accountsRDD.foreach(println)
    val df = sqlc.createDataFrame(accountsRDD)
    df.show()
    df.registerTempTable("accounts")
    sqlc.sql("select * from accounts where clientName = %s".format("\"shell\"")).show()
    sqlc.sql("select * from accounts where clientName like %s".format("\"c%\"")).show()
    sqlc.sql("select sum(balance) from accounts").show()
    import org.apache.spark.sql.functions._
    df.groupBy("country").agg(sum("balance")).show()
  }

  def style1(accountsRDD: RDD[String], sqlc: SQLContext): Unit = {
    val accountsRowRDD = accountsRDD.map(_.split(",")).map(r => Row(r(0).trim.toLong, r(1), r(2), r(3).trim.toLong))
    val aStruct = new StructType(
      Array(StructField("id", LongType, nullable = true),
        StructField("clientName", StringType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("balance", LongType, nullable = true)

      )
    )
    val df = sqlc.createDataFrame(accountsRowRDD, aStruct)
    df.show()
    print("showing accounts with balance > 15 ")
    df.filter("balance > 15").show()
  }
}


