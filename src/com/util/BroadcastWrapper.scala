package com.util

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object BroadcastWrapper {

    @volatile private var instance: Broadcast[RDD[String]] = null

    def update(sc: SparkContext, blocking: Boolean = false): Unit = {
      if (instance != null)
        instance.unpersist(blocking)
      instance = sc.broadcast(getServerInfo(sc))
    }

    def getInstance(sc: SparkContext): Broadcast[RDD[String]] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.broadcast(getServerInfo(sc))
          }
        }
      }
      instance
    }
    
    def getServerInfo(sc: SparkContext) = {
//      sc.textFile("hdfs://hacluster/user/hdfs-examples/server_info")
        sc.textFile("G:/yehao/00.test_data/DATA/server_info")
    }
  }