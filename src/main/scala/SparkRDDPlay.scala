import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by venkat kamath on 25/03/17.
  *
  * Goal of this class is to understand the behaviour of transformations that Spark provides
  */


class NumberCrunchingJob(sc:SparkContext) {

  def expMapAndFilter(): Unit = {
    val numbersRDD = sc.parallelize(List(1,2,3))
    val squaresRDD = numbersRDD.map(math.pow(_,2))
    squaresRDD.collect().foreach(println)

    val evenSquaresRDD = squaresRDD.filter(_%2==0)
    evenSquaresRDD.collect().foreach(println(_))

  }

  def expFlatMap(): Unit = {
    val numbersRDD = sc.parallelize(List(1,2,3))
    val flatNumbersRDD = numbersRDD.flatMap( n => List(n, n*2, n*3))
    flatNumbersRDD.foreach(println)
  }
}


object SparkRDDPlay {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Scala Play").setMaster("local")
    val sc = new SparkContext(conf)
    val numJob = new NumberCrunchingJob(sc)
    numJob.expMapAndFilter()
    numJob.expFlatMap()
  }

}
