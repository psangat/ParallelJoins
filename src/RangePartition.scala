import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, RangePartitioner, SparkContext, SparkConf}

/**
  * Created by PRSANGAT on 3/16/2016.
  */
object RangePartition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Range Partition and Range Search Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sortedRDD = sc.parallelize(1 to 10, 3).map(x => (x, x)).sortByKey().cache()

    sortedRDD.foreachPartition(part => {
      val tc = TaskContext.get()
      part.foreach(x => println("Partition #" + tc.partitionId() + ", Value: " + x))
    })
    countByPartition(sortedRDD).collect().foreach(println)
    // using range partition in sortedRDD
    val rp: RangePartitioner[Int, Int] = sortedRDD.partitioner.get.asInstanceOf[RangePartitioner[Int, Int]]
    // lower and upper values to search in the range
    val (lower, upper) = (1, 2)
    val range = rp.getPartition(lower) to rp.getPartition(upper)
    println("Search items are in " + range)
    val rangeFilter = (i: Int, iter: Iterator[(Int, Int)]) => {
      if (range.contains(i))
        for ((k, v) <- iter if k >= lower && k <= upper) yield (k, v)
      else
        Iterator.empty
    }
    for ((k, v) <- sortedRDD.mapPartitionsWithIndex(rangeFilter, preservesPartitioning = true).collect()) println(s"$k, $v")
  }

  def countByPartition(rdd: RDD[(Int, Int)]) = {
    rdd.mapPartitions(iter => {
      val tc = TaskContext.get
      Iterator(("Partition #" + tc.partitionId(), "Item Count: " + iter.length))
    })
  }
}
