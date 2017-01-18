/* PageRank.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.PairRDDFunctions
import scala.util.control._
import scala.util.Random
 
/* the data format is 
** URL '\t' neighbor URL
*/

//NOTICE:not use PageRank directly because it is a built-in command of scala
object MyPageRank {
  def main(args: Array[String]) {
    val begin = System.currentTimeMillis()

	  //BETTER:get the path from args
	val dataFile = "hdfs://server90:9000/user/hadoop/input/page_rank_data.txt"
	//val dataFile = "hdfs://server90:9000/user/hadoop/input/page_rank_data_small.txt"
	val conf = new SparkConf().setAppName("PageRank Application")
    val sc = new SparkContext(conf)

	val iters = 100;
	val factor = 0.15
	val epison = 0.0001
	//hdfs by default, you can also make it clear by hdfs:// or file://
    val lines = sc.textFile(dataFile, 2).cache()

	//generate the adjancy list, the separator is \t
	val links = lines.map{s =>
		val parts = s.split("\t")
		(parts(0), parts(1))
	}.groupByKey().cache()
	/*}.distinct().groupByKey().cache()*/
	/*links.foreach(println);*/

	//BETTER:use flatmap to simplify
	val mm1 = lines.map{s =>
		val parts = s.split("\t")
		(parts(0), 1)
	}.groupByKey().keys
	val mm2 = lines.map{s =>
		val parts = s.split("\t")
		(parts(1), 1)
	}.groupByKey().keys
    val mm3 = mm1.union(mm2).distinct()
	val gnum = mm3.count()
	System.err.println("the graph vertex num: " + gnum)
	
	//var ranks = links.mapValues(v => 1.0/gnum)
	var ranks = mm3.map(v => (v, 1.0/gnum))
	/*ranks.foreach(println);*/

    val load = System.currentTimeMillis()
	System.err.println("time to load data: " + (load-begin) + "ms")

	val loop = new Breaks;
	loop.breakable {
      for(i <- 1 to iters)
      {
          System.err.println("iteration num: " + i)
          val it0 = System.currentTimeMillis()

          val oldrank = ranks
          val joint = links.join(ranks).values.flatMap{ case (urls, rank) => 
              val size = urls.size
              urls.map(url => (url, rank / size))
          }
          ranks = joint.reduceByKey(_+_).mapValues(factor*(1.0/gnum) + (1-factor)*_)
          //NOTICE:joint not include the nodes with no in edges
          val more = oldrank.keys.subtract(ranks.keys).map(v => (v, factor*(1.0/gnum))).union(ranks)
          //NOTICE: intersect and diff are just foi scala, not apache spark rdds
          ranks = more
          var loss = ranks.values.reduce(_+_)
          //NOTICE+WARN:this way can not be used in functional language
          //ranks.foreach(x => loss = loss + x._2)
          loss = 1.0 - loss
          val tmp = ranks.mapValues(v => factor*(1.0/gnum) + (1-factor)*(v + loss/gnum))
          ranks = tmp
          //check if coverged
          var dist = oldrank.join(ranks).mapValues(v => (v._1-v._2)*(v._1-v._2)).values.reduce(_+_)
          //oldrank.join(ranks).values.foreach(x => dist = dist + (x._1-x._2)*(x._1-x._2))
          System.err.println("loss: " + loss)
          System.err.println("distance: " + dist)

          val it1 = System.currentTimeMillis()
          System.err.println("iteration time: " + (it1-it0) + "ms")

          if(dist < epison)
          {
              loop.break;
          }
          /*ranks.foreach(println)*/
      }
	}
    val after = System.currentTimeMillis()
    System.err.println("total loop time: " + (after-load) + "ms")
	
	//a method to sort on values
    val result = ranks.map(x => (x._2, x._1))
    //sort from high to low
    //val output = ranks.sortBy(r => (-r._2, r._1)).collect()
    val output = ranks.sortBy(r => (r._2, r._1), false).collect()
    //val output = result.sortByKey(false).map(x => (x._2, x._1)).collect()
	//val output = ranks.collect()
    output.foreach(tup => println(tup._1 + "\t" + tup._2))
	//ranks.saveAsTextFile("output-spark")  //also a directory, maybe several files

    val end = System.currentTimeMillis()
    System.err.println("Total time used: " + (end-begin) + "ms")

	sc.stop()
  }
}

