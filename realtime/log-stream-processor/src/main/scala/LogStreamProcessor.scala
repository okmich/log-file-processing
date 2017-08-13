
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream._

import org.apache.spark.streaming.flume.{FlumeUtils,SparkFlumeEvent}

import scala.util.matching.Regex

import java.text.SimpleDateFormat
import java.util.Calendar


import it.nerdammer.spark.hbase._

object LogStreamProcessor  {

	private val DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") 

	def main(args : Array[String]) : Unit = {
		if (args.length < 3){
			println("No arguments!!! Use <zookeeperHost> <streamingHost>, <port> <durationInMillis> ")
			return; 
		}

		val zookeeperHost = args(0)
		val host = args(1)
		val port = args(2).toInt

		val sparkConf = new SparkConf().setAppName("LogStreamProcessor")
		sparkConf.set("spark.hbase.host", zookeeperHost)
		sparkConf.set("zookeeper.znode.parent", "/hbase")
		val streamingCtx = new StreamingContext(sparkConf, Milliseconds(args(3).toInt))

		//do your work here
		//for each dstream, process and store the event body
		FlumeUtils.createStream(streamingCtx, host, port).
			foreachRDD(processRDD(_))

		// Start the computation
	    streamingCtx.start()
	    streamingCtx.awaitTermination()
	}


	def processRDD(rdd: RDD[SparkFlumeEvent]) : Unit = {
		//println(">>>>>>>>>>>>>>>>>>>>>>> got here " + rdd.count)
		val rddTuples = rdd.map((sfe : SparkFlumeEvent) => {
				val logEvent = new String(sfe.event.getBody.array)
				println(">>>>>>>>>>>>>>>> " + logEvent)
				val fields : Array[String] = logEvent.split(",")

				val fileName = {
					val f = fields(6)
					if (f == null || f.startsWith(".")) fields(5) else f
				}

				val ts = DATE_FORMATTER.parse(fields(1) + " " + fields(2)).getTime * -1 //get the time series in descending order
				
				(s"${fields(5)}|${ts}|${fields(0)}", //row key  - ascno|ts|ip
					fields(0),  //ip
					ts * -1,			//ts
					fields(3),	//time zone
					fields(4).toFloat.toInt,	//cik
					fields(5),	//accession number
					fileName, 	//doc
					fields(7).toFloat.toInt,	//code
					fields(8).toFloat.toInt,	//size
					if (fields(9).toFloat.toInt == 1) true else false,	//idxd
					if (fields(10).toFloat.toInt == 0) true else false,	//referred
					if (fields(11).toFloat.toInt == 0) true else false,	//has agnt value
					findMap.getOrElse(fields(12).toFloat.toInt, ""),	//find
					fields(13).toFloat.toInt,	//crawler
					if (fields.length < 15) "" else fields(14)	//browser
				)
			})

		//save to hbase
		rddTuples
		 	.toHBaseTable("log_stream:log_event")
		    .toColumns("ip","ts","tz","cik","ascno","doc","code","size","idxd","refd","agnt","find","crawler","browser")
		    .inColumnFamily("main")
		    .save()
	}


	private val findMap = Map(
			0 -> "",
			1 -> "~m/action=getcompany",
			2 -> "~m/action=getcurrent",
			3 -> "~m/Find+Companies",
			4 -> "~m/cgi-bin/srch-edgar)",
			5 -> "~m/EDGARFSClient",
			6 -> "~m/cgi-bin/current",
			7 -> "~m/Archives/edgar",
			8 -> "~m/cgi-bin/viewer",
			9 -> "~m/*-index",
			10 -> "na"
		)
}