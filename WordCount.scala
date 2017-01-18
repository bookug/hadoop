/* WordCount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
 
object WordCount {
  def main(args: Array[String]) {
/**
 * 第一步：创建Spark的配置对象SparkConf,设置Spark程序的运行时的配置信息，例如说通过setMaster来设置程序
 * 要连接的Spark集群的Master的URL,如果设置为local,则代表Spark程序在本地运行，特别适合机器配置条件非常差
 */
/*val conf = new SparkConf()*/
/**
* appname: 应用的名称
* master:主机地址
* 本地，不运行在集群值:local
* 运行在集群:spark://s0:7077
*/
/*conf.setAppName("IDEA第一个程序").setMaster("spark://master:7077")*/
/*conf.setJars(Array("file:///home/hadoop/thd/spark/test.jar"))*/

   /**
    * 根据具体的数据来源(HDFS、Hbse、local Fs、DB、S3等通过SparkContext来创建RDD)
    * RDD创建有三种方式:根据外部的数据来源例 如HDFS、根据Scala集合、由其它的RDD操作
    * 数据会被RDD划分成为一系列的Patitions,分配到每个Patition的数据属于一个Task的处理范畴
    */
	/*val logFile = "file:///home/hadoop/WordCountSmall" // Should be some file on your system*/
	val logFile = "hdfs://server90:9000/user/hadoop/input/WordCountSmall"  //should be some file in hdfs
    
	/**
	  * 创建SparkContext对象
	  * SparkContext 是Spark程序的所有功能的唯一入口，无论是采用Scal,Java,Python,R等
	  * 同时还会负则Spark程序往Master注册程序等
	  * SparkContext是整个Spark应用程序中最为至关重要的一个对象
	  */
	val conf = new SparkConf().setAppName("WordCount Application")
    val sc = new SparkContext(conf)
	
	//读取文 件，并设置为一个Patitions (相当于几个Task去执行)
    val file = sc.textFile(logFile, 2).cache()
	//map and join
	val result = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
	result.collect().foreach(x => if(x._1.length > 0) println(x._1 + "\t" + x._2))
	
	//val resultValue = result.map(x =>(x._2,x._1))
  	//val resultOrder = resultValue.sortByKey(false).map(x => (x._2 ,x._1))
  	//resultOrder.collect().foreach(x => println("key:"+ x._1 + " value:"  + x._2))
  	//result.saveAsTextFile("output")
  	//sc.stop()
  }
}

