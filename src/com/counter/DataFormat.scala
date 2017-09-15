package com.counter

import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.util.BroadcastWrapper

object DataFormat {
  
  val INDEX_CID = 0
  val INDEX_CITY = 18
  val INDEX_ISP = 23
  val INDEX_PROV = 26
  val INDEX_RATE = 27
  val INDEX_SERVER_IP = 35
  val INDEX_ROOM = 38
  
  val topic_publish = "rtmp_publish"
  val topic_relay = "request_relay"
  val separator = "\001"
  
  def getFullIsp(data : Array[String]) : String = {
    data(INDEX_ISP) + "-" + data(INDEX_PROV) + "-" + data(INDEX_CITY)
  }
    
  def joinData(ssc: StreamingContext, kafkaParams: Map[String, String] ) = {
    val dStream_publish = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topic_publish.split(",").toSet).map(f=>{
        val ary = f._2.split(separator)
        (ary(0)+"-"+ary(32), ary)
      })
        
    val dStream_relay = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topic_relay.split(",").toSet).map(f=>{
        val ary = f._2.split(separator)
        (ary(22), ary)
      })
      
     val serverInfo = BroadcastWrapper.getInstance(ssc.sparkContext)
     val serverInfoFmt = serverInfo.value.map(f=>{
       val ary = f.split(separator)
       (ary(0), ary)
     })
     
     //key为 cid-时间戳
     val dStream_relay_1 = dStream_relay.transform(f=>{
       //根据relay_ip进行关联
       f.leftOuterJoin(serverInfoFmt).map(f=>{
         if(f._2._2 != None){
            val ary = f._2._1 ++ Array(f._2._2.get(1), f._2._2.get(2), f._2._2.get(3), f._2._2.get(4))
          (ary(0)+"-"+ary(12), ary)
         }else{
           val ary = f._2._1
           (ary(0)+"-"+ary(12), ary)
         }
       })
     })
     
     dStream_publish.transformWith(dStream_relay_1, (rdd1:RDD[(String,Array[String])], rdd2:RDD[(String,Array[String])]) =>{
       
       //根据设备ip进行关联
       rdd1.leftOuterJoin(rdd2).map(f=>{ 
         if(f._2._2 != None){
           val ary = Array(f._2._2.get(22) , f._2._2.get(23) , f._2._2.get(9), f._2._2.get(26), f._2._2.get(27), f._2._2.get(28), f._2._2.get(29))
        	 (f._1, f._2._1 ++ ary)
         }else{
           (f._1, f._2._1)
         }
       })
     }).cache()
  }
  
}


  