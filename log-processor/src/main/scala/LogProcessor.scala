import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.matching.Regex

case class Log (host: String, clientAuthId: String, userId: String, ts: String, tz: String, method: String, resource: String, protocol:String, responsecode:String, bytes:String)

object LogProcessor  {

	def main(args : Array[String]) : Unit = {
		if (args.length != 2){
			println("No arguments!!! Use <inputPath> <outputFolderPath>")
			return; 
		}

		val inputPath = args(0)
		val outputPath = args(1)

		val conf = new SparkConf().setAppName("NasaLogProcessor")
		conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		conf.registerKryoClasses(Array(classOf[Log]))
		val sc = new SparkContext(conf)

		val logInputRdd = sc.textFile(inputPath)
		val LGREGEXP = "(.+?)\\s(.+?)\\s(.+?)\\s\\[(.+?)\\s(.+?)\\]\\s\"(.+?)\\s(.+?)\\s(.+?)\"\\s(.+?)\\s(.+?)".r
		val logRDD = logInputRdd.map(_ match {
			case LGREGEXP(host, clientAuthId, userId, ts, tz, method, resource, protocol, responsecode, bytes) => Log(host, clientAuthId, userId, ts, tz, method, resource, protocol, responsecode, bytes)
			case _ => Log("", "", "", "", "", "", "", "", "", "")
		}).filter (_.host != "")

		val sqlContext = new SQLContext(sc)

		//defined schema for the final output
		val schema = 
			StructType(
					StructField("host", StringType, false) ::
					StructField("clientAuthId", StringType, false) ::
					StructField("userId", StringType, false) ::
					StructField("ts", StringType, false) ::
					StructField("tz", StringType, false) ::
					StructField("method", StringType, false) ::
					StructField("resource", StringType, false) ::
					StructField("protocol", StringType, false) ::
					StructField("responsecode", StringType, false) ::
					StructField("bytes", StringType, false) :: Nil
				)
		val logRowRDD = logRDD map (f => {
				Row(f.host, f.clientAuthId, f.userId, f.ts, f.tz, f.method, f.resource, f.protocol, f.responsecode, f.bytes)
			})
		//create the log dataframe
		val logDF = sqlContext.createDataFrame(logRowRDD, schema)

		logDF.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(outputPath)
	}
}