import org.apache.spark.{TaskContext, SparkContext, SparkConf}

/**
  * Created by PRSANGAT on 3/15/2016.
  */
object ParallelJoinSpark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Parallel Join").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val t2 = sc.parallelize(List(1 -> "one", 2 -> "two", 3 -> "three", 4 -> "four", 5 -> "five", 6 -> "six", 7 -> "seven", 8 -> "eight", 9 -> "nine", 10 -> "ten"), 6)
    val t1 = Map(1 -> "uno", 4 -> "test", 5 -> "tiger", 6 -> "delete")

    val bt1 = sc.broadcast(t1)
    t2.foreachPartition(p => {
      val tc = TaskContext.get
      val partId = tc.partitionId() + 1
      val taskId = tc.taskAttemptId()
      p.foreach(rec => {
        val filteredValue = bt1.value.filterKeys(_.equals(rec._1))
        val mappedFilterValue = filteredValue.map { case (k, v) => ("Key: " + k, " Values: " + (v, rec._2), " Task ID: " + taskId, " PartitionId: " + partId) }
        mappedFilterValue.foreach(println)
      })
    })
  }
}
