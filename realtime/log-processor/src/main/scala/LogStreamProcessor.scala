
import org.apache.spark.{SparkConf,SparkContext}

import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream._

import scala.util.matching.Regex

import java.text.SimpleDateFormat
import java.util.Calendar

object LogStreamProcessor  {

	def main(args : Array[String]) : Unit = {
		if (args.length < 1){
			println("No arguments!!! Use <zookeeperHost>")
			return; 
		}

		val inputPath = args(0)
		val outputPath = args(1)

		val sparkConf = new SparkConf().setAppName("LogStreamProcessor")
		sparkConf.set("spark.hbase.host", args(0))
		sparkConf.set("zookeeper.znode.parent", "/hbase")
		val streamingCtx = new StreamingContext(sparkConf, Milliseconds(250))

		//do your work here



		// Start the computation
	    streamingCtx.start()
	    streamingCtx.awaitTermination()
	}
}