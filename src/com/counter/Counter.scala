package com.counter

import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import com.util.SQLContextSingleton
import com.Anlyse

object Counter {
  //计算 异常信息: 设备id  和  运营商
  def countCidIsp(data:DStream[Array[String]], windowDuration: Duration): DStream[(String, (String, Int, String, Long))]={
    val cid_isp_count = data.map(f=>{
      (f(0), (1, DataFormat.getFullIsp(f)))
    }).reduceByKeyAndWindow(
        (x:(Int, String),y:(Int, String))=>{
          (x._1 + y._1, x._2)
        }, 
        windowDuration, Anlyse.slideDuration)
        
    //转换为 (isp, ( cid, count))
     val isp_key = cid_isp_count.map(f=>{
        (f._2._2, (f._1, f._2._1))
     })
     
     val isp_count = data.transform(rdd=>{
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._
        //构造case class: IpRoomInfo,提取相应的字段
        val logDataFrame = rdd.map(w => CidIspInfo(w(DataFormat.INDEX_CID),DataFormat.getFullIsp(w))).toDF()
       //注册为tempTable
        logDataFrame.registerTempTable("cidispinfo")
        //查询该批次的pv,ip数,uv
        val logCountsDataFrame =
          sqlContext.sql("select isp, count(distinct cid) as ip_count from cidispinfo group by isp")
          
          logCountsDataFrame.map(f=>{
            (f.getString(0), f.getLong(1))
          })
     })
     
    isp_key.leftOuterJoin(isp_count).map(f=>{
      (f._2._1._1, (f._2._1._1, f._2._1._2, f._1, f._2._2.getOrElse(0)))
    })
    
  }
  
  //计算异常信息: 服务器 和 机房  
  def countIpRoom(data: DStream[(String, Array[String])], windowDuration: Duration): DStream[(String, (String, Int, String, Long))]={
    
    val win_ip_count = data.map(f=>{
      (f._2(DataFormat.INDEX_SERVER_IP),(1,f._2(DataFormat.INDEX_ROOM)))
    }).reduceByKeyAndWindow(
        (x:(Int, String),y:(Int, String))=>{
          (x._1 + y._1, x._2)
        }, 
        windowDuration, Anlyse.slideDuration)
        
    //转换为 (机房, ( ip, count))
    val room_key = win_ip_count.map(f=>{
      (f._2._2, (f._1, f._2._1))
    })
    
    val room_count = data.transform(rdd=>{
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: IpRoomInfo,提取相应的字段
      val logDataFrame = rdd.map(w => IpRoomInfo(w._2(DataFormat.INDEX_SERVER_IP),w._2(DataFormat.INDEX_ROOM))).toDF()
      //注册为tempTable
      logDataFrame.registerTempTable("iproominfo")
      //查询该批次的pv,ip数,uv
      val logCountsDataFrame =
        sqlContext.sql("select room, count(distinct ip) as ip_count from iproominfo group by room")
        
        logCountsDataFrame.map(f=>{
          (f.getString(0), f.getLong(1))
        })
    })
    
    room_key.leftOuterJoin(room_count).map(f=>{
      (f._2._1._1, (f._2._1._1, f._2._1._2, f._1, f._2._2.getOrElse(0)))
    })
    
  }
  
  def transformCidIsp(cidIsp: DStream[(String, (String, Int, String, Long))], cidIspAll: DStream[(String, (String, Int, String, Long))]) = {
    
    val cidIspResult = cidIsp.leftOuterJoin(cidIspAll).map(f=>{
      val icd = f._1
      val isp = f._2._1._3
      val cidIsp = f._2._1
      val cidIspAll = f._2._2.getOrElse("", 0, "", 0L)
      var errorType:String = ""
      if(cidIsp._4 == cidIspAll._4 && cidIsp._2 == cidIspAll._2){
        errorType = "isp"
      }else if(cidIsp._4 < cidIspAll._4 && cidIsp._2 == cidIspAll._2){
        errorType = "cid"
      }
      
      (errorType, (icd, isp))
    })
    
    val ispData = cidIspResult.filter(f=>{
      f._1=="isp"
    }).map(f=>{
      (f._2._2, f._2._1)
    })
    ispData.print()
    
    val cidData = cidIspResult.filter(f=>{
      f._1=="cid"
    }).map(f=>{
      f._2
    })
    cidData.print()
    
    (cidData, ispData)
  }
  
  def transformRoomInfo(ipRoom: DStream[(String, (String, Int, String, Long))], ipRoomAll: DStream[(String, (String, Int, String, Long))]) = {
    
    val ipRoomResult = ipRoom.leftOuterJoin(ipRoomAll).map(f=>{
      val ip = f._1
      val room = f._2._1._3
      val ipRoom = f._2._1
      val ipRoomAll = f._2._2.getOrElse("", 0, "", 0L)
      var errorType:String = ""
      if(ipRoom._4 == ipRoomAll._4 && ipRoom._2 == ipRoomAll._2){
        errorType = "room"
      }else if(ipRoom._4 < ipRoomAll._4 && ipRoom._2 == ipRoomAll._2){
        errorType = "ip"
      }
      
      (errorType, (room, ip))
    })
    
    val roomData = ipRoomResult.filter(f=>{
      f._1=="room"
    }).map(f=>{
      f._2
    })
    roomData.print()
    
    val ipData = ipRoomResult.filter(f=>{
      f._1=="ip"
    }).map(f=>{
      (f._2._2, f._2._1)
    })
    ipData.print()
    
    (ipData, roomData)
  }

  /**
   * 过滤掉 异常类型为 room 和 serverip 的数据
   */
  def filterData(data: DStream[(String, Array[String])], windowDuration: Duration, ipRoomResult: (DStream[(String, String)], DStream[(String, String)]))={
    //去掉room 的数据
    val rateFiltedData = data.window(windowDuration, Anlyse.slideDuration).map(f=>{
      (f._2(DataFormat.INDEX_ROOM), f._2)
    }).leftOuterJoin(ipRoomResult._2).filter(f=>{
      f._2._2 == None
    }).map(f=>{
      (f._2._1(DataFormat.INDEX_SERVER_IP), f._2._1)
    }).leftOuterJoin(ipRoomResult._1).filter(f=>{
      f._2._2 == None
    }).map(f=>{
      f._2._1
    })
    
    rateFiltedData
 
//测试可用
//    val ds1 = data.window(windowDuration, Anlyse.slideDuration).map(f=>{
//      (f._2(DataFormat.INDEX_ROOM), f._2)
//    })
//    ds1.print()
//    ipRoomResult._2.print()
//    ds1.leftOuterJoin(ipRoomResult._2).print()
//    
//    val ds2 = ds1.leftOuterJoin(ipRoomResult._2).filter(f=>{
//      f._2._2 == None
//    }).map(f=>{
//      (f._2._1(DataFormat.INDEX_SERVER_IP), f._2._1)
//    })
//    ds2.print()
//    ds2.leftOuterJoin(ipRoomResult._1).print()
//    val ds3 = ds2.leftOuterJoin(ipRoomResult._1).filter(f=>{
//      f._2._2 == None
//    }).map(f=>{
//      f._2._1
//    })
//    ds3.print()
    
//    ds3.foreachRDD(f=>{
//      f.foreach(f=>{
//        println("f.mkString:"+f.mkString(","))
//      })
//    })
//    ds3
  }
}

case class IpRoomInfo(ip:String, room:String)
case class CidIspInfo(cid:String, isp:String)