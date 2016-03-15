import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by PRSANGAT on 3/15/2016.
  */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Parallel Join").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Applications\\Infolder\\wc.txt")
    lines.foreach(println)
    val words = lines.flatMap(line => line.split(" ").map(word => (word, 1))).reduceByKey(_ + _)
    words.foreach(println)
  }
}
