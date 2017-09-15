package com.store

import org.apache.spark.streaming.dstream.DStream
import com.util.BroadcastWrapper
import com.util.HbaseUtils
import com.DateUtils

object Store {
  val columnFamily = "cf"
  val zkClientPort = "24002"
  val zkQuorum = "node243,node242,node241"
  
  def storeData(ipRoom: (DStream[(String, String)], DStream[(String, String)]), cidIspResult: (DStream[(String, String)], DStream[(String, String)])) = {
    val ip = ipRoom._1.map(f=>{
      (DateUtils.getNowTime()+f._1, Array(("ip_server", f._1)))
    })
    ip.foreachRDD(rdd=>{
    	HbaseUtils.truncate(zkClientPort, zkQuorum, "send_rate_anlyse_ip");
      rdd.foreachPartition(iterator => HbaseUtils.hBaseWriter(iterator, zkClientPort, zkQuorum, columnFamily, "send_rate_anlyse_ip"))
    })
    
    val room = ipRoom._2.map(f=>{
      (DateUtils.getNowTime()+f._1, Array(("room", f._1)))
    })
    room.foreachRDD(rdd=>{
    	HbaseUtils.truncate(zkClientPort, zkQuorum, "send_rate_anlyse_room");
      rdd.foreachPartition(iterator => HbaseUtils.hBaseWriter(iterator, zkClientPort, zkQuorum, columnFamily, "send_rate_anlyse_room"))
    })
    
    val cid = cidIspResult._1.map(f=>{
      (DateUtils.getNowTime()+f._1, Array(("cid", f._1)))
    })
    cid.foreachRDD(rdd=>{
    	HbaseUtils.truncate(zkClientPort, zkQuorum, "send_rate_anlyse_cid");
      rdd.foreachPartition(iterator => HbaseUtils.hBaseWriter(iterator, zkClientPort, zkQuorum, columnFamily, "send_rate_anlyse_cid"))
    })
    
    val isp = cidIspResult._2.map(f=>{
      (DateUtils.getNowTime()+f._1, Array(("isp", f._1)))
    })
    isp.foreachRDD(rdd=>{
    	HbaseUtils.truncate(zkClientPort, zkQuorum, "send_rate_anlyse_isp");
      rdd.foreachPartition(iterator => HbaseUtils.hBaseWriter(iterator, zkClientPort, zkQuorum, columnFamily, "send_rate_anlyse_isp"))
    })
  }
  
  def storeJoinedData(result: DStream[(String, Array[String])]) = {
    //数据为key、value格式
     val result_1 = result.map(f=>{
        	 (f._1, getCol.zip(f._2))
         })
         
     result_1.foreachRDD(rdd => {
       
       // driver端运行，涉及操作：广播变量的初始化和更新
        // 可以自定义更新时间
        if ((DateUtils.getNowTime().split(" ")(1) >= "14:35:00") && (DateUtils.getNowTime().split(" ")(1) <= "14:40:00")) {
          BroadcastWrapper.update(rdd.sparkContext, true)
          println("广播变量更新成功： " + DateUtils.getNowTime())
        }
        
      //partition运行在executor上,关联后的数据写入到hbase
      rdd.foreachPartition(iterator => HbaseUtils.hBaseWriter(iterator, zkClientPort, zkQuorum, columnFamily, "send_rate_anlyse"))
    })
    
  }
  
  def getCol = {
    Array("peer_id",
    		"app_id",
    		"beat",
    		"bitrate",
    		"broker",
    		"client_type",
    		"create_at",
    		"framerate",
    		"geoip",
    		"host",
    		"input_type",
    		"ip",
    		"log_len",
    		"log_type",
    		"msg_cmd",
    		"port",
    		"public_ip_area",
    		"public_ip_cc",
    		"public_ip_city",
    		"public_ip_code",
    		"public_ip_continent",
    		"public_ip_country",
    		"public_ip_en",
    		"public_ip_isp",
    		"public_ip_latitude",
    		"public_ip_longitude",
    		"public_ip_prov",
    		"send_rate",
    		"send_time",
    		"session",
    		"tags",
    		"time_toal",
    		"timestamp",
    		"viewers",
    		"wait_time",
    		"relay_ip",
    		"relay_port",
    		"returnvalue",
    		"server_machine_room",
    		"server_ip_prov",
    		"server_ip_city",
    		"server_ip_isp"
      )
  }
}