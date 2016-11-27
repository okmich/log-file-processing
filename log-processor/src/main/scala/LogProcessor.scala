import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.matching.Regex

import java.text.SimpleDateFormat
import java.util.Calendar

case class Log (host: String, clientAuthId: String, userId: String, method: String, resource: String, protocol:String, responsecode:String, bytes:String, tz: String, ts: String, year: Short, month: Short, day: Short, hour: Short, minute: Short, sec: Short, dayOfWeek: Short)

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

		val badRecords = sc.accumulator(0, "Bad Log Line Count")

		val logRDD = logInputRdd.mapPartitions(iters => {
			val cal  = Calendar.getInstance
			val sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss")

			val ZERO = (0).toShort
			
			iters.map (_ match {
				case LGREGEXP(host, clientAuthId, userId, ts, tz, method, resource, protocol, responsecode, bytes) => {
						cal.setTime(sdf.parse(ts))
						val year = cal.get(Calendar.YEAR).toShort
						val month = cal.get(Calendar.MONTH).toShort
						val day= cal.get(Calendar.DAY_OF_MONTH).toShort
						val hour = cal.get(Calendar.HOUR).toShort
						val min = cal.get(Calendar.MINUTE).toShort
						val sec = cal.get(Calendar.SECOND).toShort
						val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK).toShort
						
						Log(host, clientAuthId, userId, method, resource, protocol, responsecode, bytes, tz, ts, year, month, day, hour, min, sec, dayOfWeek)
					}
				case _ => Log("", "", "", "", "", "", "", "", "", "", ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, ZERO) //bad records
			}).filter (l => {
				//put an accumulator
				badRecords += 1
				
				_.host != ""
			}) //removing bad records
		})

		val sqlContext = new SQLContext(sc)

		//defined schema for the final output
		val schema = 
			StructType(
					StructField("host", StringType, false) ::
					StructField("clientAuthId", StringType, false) ::
					StructField("userId", StringType, false) ::
					StructField("method", StringType, false) ::
					StructField("resource", StringType, false) ::
					StructField("protocol", StringType, false) ::
					StructField("responsecode", StringType, false) ::
					StructField("bytes", StringType, false) ::
					StructField("tz", StringType, false) :: 
					StructField("ts", StringType, false) :: 
					StructField("ts_year", ShortType, false) :: 
					StructField("ts_month", ShortType, false) :: 
					StructField("ts_day", ShortType, false) :: 
					StructField("ts_hour", ShortType, false) :: 
					StructField("ts_minute", ShortType, false) :: 
					StructField("ts_sec", ShortType, false) :: 
					StructField("ts_dayOfWeek", ShortType, false) ::  Nil
				)
		val logRowRDD = logRDD map (f => {
				Row(f.host, f.clientAuthId, f.userId, f.method, f.resource, f.protocol, f.responsecode, f.bytes, f.tz, f.ts, f.year, f.month, f.day, f.hour, f.minute, f.sec, f.dayOfWeek)
			})
		//create the log dataframe
		val logDF = sqlContext.createDataFrame(logRowRDD, schema)

		logDF.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(outputPath)
	}
}