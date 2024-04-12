package project_3

import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g: Graph[Int, Int]): Graph[Int, Int] = {
      // Initial mapping: -1 for all vertices (not in MIS), with a random number.
      var mainGraph = g.mapVertices { case (id, _) => (-1, scala.util.Random.nextDouble()) }.cache()

      var iteration = 0
      do {
          iteration += 1
          println(s"=======================================================================================Iteration: $iteration")

          // Create a processing subgraph that includes only vertices marked as -1 in the main graph.
          val processingGraph = mainGraph.subgraph(vpred = (_, attr) => attr._1 == -1).cache()

          // Step 1: Identify local maximums within the processing subgraph.
          val messages = processingGraph.aggregateMessages[Double](
              triplet => {
                  triplet.sendToDst(triplet.srcAttr._2)
                  triplet.sendToSrc(triplet.dstAttr._2)
              },
              (a, b) => math.max(a, b) // Choose the maximum random number.
          ).cache()

          // Step 2: Update vertices in the main graph based on the maximum message received.
          mainGraph = mainGraph.outerJoinVertices(messages) {
              (id, oldAttr, msgOpt) => msgOpt match {
                  case Some(msg) if oldAttr._2 >= msg && oldAttr._1 == -1 =>
                      (1, oldAttr._2) // Mark as in MIS.
                  case _ => oldAttr
              }
          }.cache()

          // Mark neighbors of MIS vertices as ineligible by sending a signal (marking them with 0).
          val neighborSignals = mainGraph.aggregateMessages[Int](
              triplet => {
                  if (triplet.srcAttr._1 == 1 && triplet.dstAttr._1 == -1) {
                      triplet.sendToDst(0) // Signal to mark as ineligible.
                  } else if (triplet.dstAttr._1 == 1 && triplet.srcAttr._1 == -1) {
                      triplet.sendToSrc(0) // Signal to mark as ineligible.
                  }
              },
              (a, b) => a // The actual message value is not important; presence of a message marks ineligibility.
          ).cache()

          // Apply the neighbor signals in the main graph.
          mainGraph = mainGraph.outerJoinVertices(neighborSignals) {
              (id, oldAttr, signalOpt) => signalOpt match {
                  case Some(_) if oldAttr._1 == -1 =>
                      (0, oldAttr._2) // Mark as ineligible (not for MIS).
                  case _ => oldAttr
              }
          }.cache()

          processingGraph.unpersist(blocking = false)
          messages.unpersist(blocking = false)
          neighborSignals.unpersist(blocking = false)

          println(s"=======================================================================================Iteration: $iteration - Remaining active vertices to process.")
      } while (mainGraph.vertices.filter { case (_, attr) => attr._1 == -1 }.count() > 0)

      // After all iterations, adjust the main graph to indicate MIS membership.
      // Convert all vertices marked as 0 back to -1, except those marked as 1.
      val finalGraph = mainGraph.mapVertices {
          case (_, (status, _)) => if (status == 0) -1 else status
      }.cache()

      println("=======================================================================================Final Graph Vertices:")
      finalGraph.vertices.collect().foreach(println)

      finalGraph
  }






  def verifyMIS(graph: Graph[Int, Int]): Boolean = {
    // Step 1: Each vertex sends its MIS status to all neighbors
    val messages = graph.aggregateMessages[Array[Int]](
      triplet => {
        // Sending 2-tuple information as per instructions could be interpreted as sending its own value to both sides
        triplet.sendToDst(Array(triplet.srcAttr))
        triplet.sendToSrc(Array(triplet.dstAttr))
      },
      // Aggregate messages at each vertex
      (a, b) => a ++ b // Concatenate arrays to have a list of all neighbor statuses
    )

    // Step 2: Verify the conditions for all vertices
    val verification = graph.vertices.leftJoin(messages) {
      case (vertexId, vertexValue, Some(neighbors)) => 
        if (vertexValue == 1) {
          // If the vertex is in the MIS, no neighbor should be in the MIS
          !neighbors.contains(1)
        } else {
          // If the vertex is not in the MIS, it should have at least one neighbor in the MIS
          neighbors.contains(1)
        }
      case (_, _, None) => false // If no messages, condition fails
    }

    // Step 3: Check for overall validity (no vertex should invalidate the conditions)
    verification.filter(!_._2).count() == 0 // True if all vertices meet conditions, False otherwise
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==========================================================================================================================================================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      println("SUCCESFUL CSV WRITTEN")
      println("==========================================================================================================================================================================")
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      println("==================================")
      if(ans) {
        println("Yes")
        println("==================================")}
      else{
      
        println("No")
        println("==================================")}
    }
    else
    {
        println("==================================")
        println("Usage: project_3 option = {compute, verify}")
        println("==================================")
        sys.exit(1)
    }
  }
}
