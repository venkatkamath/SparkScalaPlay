import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by venkat on 25/03/17.
  *
  * to setup tables in cassandra, refer to commands in cassandra_db_setup.txt
  *
  * Goal of this class is to read and write DF/RDD against cassandra
  */


object SparkCassandraPlay {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Cassandra Play").setMaster("local")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val job = new SparkCassandraJob(sc)
    job.run()
  }
}


class SparkCassandraJob(sc: SparkContext) {

  def readFromCassandra(sc: SparkContext): Unit = {
    val empRDD = sc.cassandraTable("dev","emp")
    empRDD.foreach(print)
  }

  def saveToCassandra(sc: SparkContext): Unit = {
    val accountsStrRDD = sc.textFile("./accounts.csv")
    val currenciesStrRDD = sc.textFile("./currencies.csv")
    val sqlc = new SQLContext(sc)
    val accountsDF = sqlc.createDataFrame(accountsStrRDD.map(s => s.split(","))
      .map(r => Account(r(0).trim.toLong, r(1).trim, r(2).trim, r(3).trim.toLong, r(4).trim.toLong)))
    val currenciesDF = sqlc.createDataFrame(currenciesStrRDD.map(s => s.split(","))
      .map(r => Currency(r(0).trim.toLong, r(1).trim)))
    val accountBalanceDF = accountsDF.join(currenciesDF, Seq("currencyId"), "leftouter")
    //accountBalanceDF.createCassandraTable("dev","account_balance")
    implicit val rowWriter = SqlRowWriter.Factory
    accountBalanceDF.rdd.saveToCassandra("dev", "account_balance",
      SomeColumns("currencyid","id","clientname", "country","balance","currency"))
  }

  def run(): Unit = {
    readFromCassandra(sc)
    saveToCassandra(sc)
  }

}


