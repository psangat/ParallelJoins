import org.apache.spark._
import org.apache.spark.rdd.RDD

class CustomPartitioner(numParts: Int) extends Partitioner {
  require(numParts >= 0, s"Number of partitions ($numParts) cannot be negative.")

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => (key.hashCode % numPartitions)
  }

  // Java equals method to let Spark compare our Partitioner objects
  override def equals(other: Any): Boolean = other match {
    case h: CustomPartitioner =>
      (h.numPartitions == numPartitions)
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

  override def numPartitions: Int = numParts
}

/**
  * Created by subodhs on 28/03/2016.
  * Modified by psangat on 4/5/2016.
  */
object HashPartition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hash Partitions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val collections = sc.parallelize(for {
      x <- 1 to 3
      y <- 1 to 2
    } yield (x, y), 1)

    def getList() = List(
      1 -> "one", 2 -> "Two")

    val hp = new CustomPartitioner(5)
    val rddHp = collections.partitionBy(hp)
    var rddHp2 = sc.parallelize(getList()).partitionBy(hp)
    val btHp2 = sc.broadcast(rddHp2)
    printRddCollection(rddHp)
    // Try to implement temp table which keep index of partitions and index.
    rddHp.foreachPartition(part => {
      val tc = TaskContext.get
      val partId = tc.partitionId() + 1
      val taskId = tc.taskAttemptId()

      part.foreach(record => {
        val filteredValue = btHp2.value.mapPartitionsWithIndex((idx, iter) => if (idx == 1) iter else Iterator(), true)
        // If the partition number is not 1 ignore data. The assumption is we will know the partition number.
        // This basically will be on-the-fly indexing of the partitions so we have to be sure that
        // it will have the same partition number all the time which i think will be as it is hash partition based on key (fingers crossed)
        // However, i have set the preserves partitioning to be true which should do the trick.
        filteredValue.filter(p => p._1.equals(record._1)).foreach(println)
      })
    })
  }

  def printRddCollection(collections: RDD[(Int, Int)]) = {
    val tc = TaskContext.get
    collections.foreachPartition(part => {
      val tc = TaskContext.get
      part.foreach(x => println("Partition #:" + tc.partitionId() + "    Values : " + x))
    })
  }
}
