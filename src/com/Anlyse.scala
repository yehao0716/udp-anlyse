package com

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import com.counter.Counter
import com.counter.DataFormat
import com.store.Store


object Anlyse {
  //20 300 192.168.160.246:21005,192.168.160.245:21005,192.168.160.244:21005
	
	val windowDuration = Seconds(3600)
	val batchDuration = Seconds(60)
	val slideDuration = Seconds(60)
			
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.160.246:21005,192.168.160.245:21005,192.168.160.244:21005"

    // Create a Streaming startup environment.
    val sparkConf = new SparkConf()
    sparkConf.setAppName("DataSightStreamingExample").setMaster("local")
    val ssc = new StreamingContext(sparkConf, batchDuration)
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
      
    //关联数据
    val joinData = DataFormat.joinData(ssc, kafkaParams)
    //保存关联数据到hbase 
    Store.storeJoinedData(joinData)
    
    val ipRoomAll = Counter.countIpRoom(joinData.window(windowDuration, slideDuration), windowDuration)
    
    val joinDataRate = joinData.filter(f=>{
      f._2(DataFormat.INDEX_RATE).toFloat > 0.5
    }).cache()
    
    val ipRoom = Counter.countIpRoom(joinDataRate.window(windowDuration, slideDuration), windowDuration)
    
    val ipRoomResult = Counter.transformRoomInfo(ipRoom, ipRoomAll)
    
    val cidIspRateData = Counter.filterData(joinDataRate, windowDuration, ipRoomResult)
    
    val cidIspData = Counter.filterData(joinData, windowDuration, ipRoomResult)
    
    val cidIspAll = Counter.countCidIsp(cidIspData, windowDuration)
    val cidIsp = Counter.countCidIsp(cidIspRateData, windowDuration)
    
    val cidIspResult = Counter.transformCidIsp(cidIsp, cidIspAll)
    
    // 保存到Hbase
    Store.storeData(ipRoomResult, cidIspResult)
    
    // The Streaming system starts.
    ssc.start()
    ssc.awaitTermination()
  }
}

  