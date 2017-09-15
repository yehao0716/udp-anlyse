package com.util

import java.io.IOException
import java.util.ArrayList

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Admin
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

object HbaseUtils {
  val LOG = Logger.getLogger(HbaseUtils.getClass)
  def truncate(zkClientPort: String, zkQuorum: String, tablename:String) ={
    
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zkClientPort)
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    var connection: Connection = null
    
    var admin:Admin = null
    val tableName = TableName.valueOf(tablename)
    try {
    	connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin();
      if(admin.tableExists(tableName)){
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }       
      LOG.info("Drop table successfully.");
      
      val tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(tableDescriptor);
      
    } catch {
      case e: IOException =>
        e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          // 关闭Hbase连接.
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  
  }
  
   /**
   * 在executor端写入数据
   * @param iterator  消息
   * @param zkClientPort
   * @param zkQuorum
   * @param columnFamily
   */
  def hBaseWriter(iterator: Iterator[(String, Array[(String,String)])], zkClientPort: String, zkQuorum: String, columnFamily: String, tablename:String): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", zkClientPort)
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    var table: Table = null
    var connection: Connection = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      // Specify the table name.
      table = connection.getTable(TableName.valueOf(tablename))
      
      val iteratorArray = iterator.toArray
      val rowList = new ArrayList[Get]()
      
      val familyname = Bytes.toBytes(columnFamily)
      
      
      val putList = new ArrayList[Put]()
      for (i <- 0 until iteratorArray.size) {
        val row = iteratorArray(i)
        val put = new Put(Bytes.toBytes(row._1))
        for(data <- row._2){
          put.addColumn(familyname, Bytes.toBytes(data._1), Bytes.toBytes(data._2))
        }
        putList.add(put)
      }
      if (putList.size() > 0) {
        table.put(putList)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          // 关闭Hbase连接.
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }
}