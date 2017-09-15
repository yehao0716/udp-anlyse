package com

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {
  
  def getNowTime() : String = {
    val date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")  
    date.format(new Date)  
  }
  
}