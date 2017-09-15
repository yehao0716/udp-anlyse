package com.util

import com.DateUtils
import java.util.Date
import java.io.PrintWriter
import java.io.FileWriter

object DataGenerater {
	val publish = "#CID#\001Test\001null\00133\001ly_log_mq\0011\0012017-09-01 08:04:29\00113\001null\001null\001null\001#IP#\00176\001rtmp_publish\001784\00160137\001\001CN\001#CITY#\001140100\001亚洲\001中国\001China\001#ISP#\00137.857014\001112.549248\001#PROV#\001#RATE#\00165\001950576595\001null\0015012\001#TIME#\0010\0014947\001"
	val relay = "#CID#\00176\001171.120.139.70\001Test\001太原\0011049878616\001中国\00138177\001780\0014\001112.549248\00137.857014\001#TIME#\001亚洲\001联通\001request_relay\001China\0012017-09-01 17:53:53\001\001CN\0011\00146562\001#SERVER_IP#\001140100\001山西\001ly_log_mq\001"
	
	def main(args: Array[String]): Unit = {

	  val cids = Array(("2003471","171.120.236.189","湖北","宜昌","联通"),
        ("2003472","171.120.236.190","湖北","宜昌","联通"),
        ("2003473","171.120.236.191","湖北","宜昌","联通"),
        ("2003474","171.120.236.192","湖北","宜昌","移动"),
        ("2000001","151.151.1.1","湖北","武汉","移动"),
        ("2000002","151.151.1.2","湖北","武汉","联通")
        )
    
    val rate = Array("0.1", "0.6")
    val serverIps = Array(("100.1.1.1", "机房4"),
          ("100.1.1.2", "机房5"),
          ("100.1.1.3", "机房6"),
          ("122.0.0.0", "机房1"),
          ("122.0.0.3", "机房1"),
          ("122.0.0.1", "机房2"),
          ("122.0.0.2", "机房2"),
          ("123.1.1.1", "机房3")
          )
    
    val time = new Date().getTime.toString()
    
    //生成运营商异常的数据
    writer(cids(0), time, rate(1), serverIps(0)._1);
    writer(cids(1), time, rate(1), serverIps(0)._1);
    writer(cids(2), time, rate(1), serverIps(0)._1);
    writer(cids(3), time, rate(0), serverIps(0)._1);
	  
  }
  
  def writer(cid:(String, String, String, String, String), time:String, rate:String, serverIp:String) = {
    val writer = new FileWriter("G:\\yehao\\00.test_data\\DATA\\generate\\rtmp_publish", true)
    val publishStr = publish.replace("#CID#", cid._1).replace("#IP#", cid._2).replace("#PROV#", cid._3).replace("#CITY#", cid._4).replace("#ISP#", cid._5).replace("#TIME#", time).replace("#RATE#", rate) + "\n"
    writer.write(publishStr)
    writer.close()
    
    val writerRelay = new  FileWriter("G:\\yehao\\00.test_data\\DATA\\generate\\request_relay", true)
    val relayStr = relay.replace("#CID#", cid._1).replace("#SERVER_IP#", serverIp).replace("#TIME#", time) + "\n"
    writerRelay.write(relayStr)
    writerRelay.close()
    
  }
  
}