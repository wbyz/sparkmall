package com.atguigu.bigdata.sparkmall.common.util

/**
  * @author Witzel
  * @since 2019/7/15 10:39
  */
object StringUtil {
    def isNotEmpty(s: String): Boolean = {
        s != null && !"".equals(s.trim) //trim只能去掉半角空格，全角空格去不掉
    }
}
