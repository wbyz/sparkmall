package com.atguigu.bigdata.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author Witzel
  * @since 2019/7/16 15:45
  */
object DateUtil {

    def formatStringByTimestamp(ts : Long, f : String = "yyyy-MM-dd HH:mm:ss") :String ={

        formatStringByDate(new Date(ts),f)

    }
    def formatStringByDate(d:Date, f : String = "yyyy-MM-dd HH:mm:ss"):String ={
        val format = new SimpleDateFormat(f)
        format.format(d)

    }
        def parseLongByString(s:String,f : String = "yyyy-MM-dd HH:mm:ss"):Long={
            pareseDateByString(s,f).getTime
        }
    def pareseDateByString(s:String,f : String = "yyyy-MM-dd HH:mm:ss"):Date={
        val format = new SimpleDateFormat(f)
        format.parse(s)
    }

}
