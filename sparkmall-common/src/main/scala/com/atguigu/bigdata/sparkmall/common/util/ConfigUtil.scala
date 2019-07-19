package com.atguigu.bigdata.sparkmall.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}



// 配置工具类
object ConfigUtil {

    // 专门获取properties文件，所以后缀不用加
    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
    private val condBundle: ResourceBundle = ResourceBundle.getBundle("condition")

    def main(args: Array[String]): Unit = {
//                println(getValueByKey("hive.database"))
        println(getValueByJsonKey("startDate"))
    }

    /**
      * 从条件中获取数据
      * @param jsonKey
      * @return
      */
    def getValueByJsonKey(jsonKey : String): String = {
        val jsonString = condBundle.getString("condition.params.json")
        // 解析json字符串
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        jsonObject.getString(jsonKey)
    }

    /**
      * 从配置文件中，根据keu获取value
      *
      * @param key
      * @return
      */
    def getValueByKey(key: String): String = {

        bundle.getString(key)

/*        val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

        val properties = new Properties()
        properties.load(stream)

        properties.getProperty(key)
*/
    }


    /*    // FileBasedConfigurationBuilder:产生一个传入的类的实例对象
        // FileBasedConfiguration:融合FileBased与Configuration的接口
        // PropertiesConfiguration:从一个或者多个文件读取配置的标准配置加载器
        // configure():通过params实例初始化配置生成器
        // 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过params参数对其初始化


        def apply(propertiesName:String) = {
            val configurationUtil = new ConfigurationUtil()
            if (configurationUtil.config == null) {
                configurationUtil.config = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
                        .configure(new Parameters().properties().setFileName(propertiesName)).getConfiguration
            }
            configurationUtil
        }*/

}

/*class ConfigurationUtil(){
    var config:FileBasedConfiguration=null

}*/

