package com.atguigu.bigdata.sparkmall.realtime


import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author Witzel
  * @since 2019/7/16 14:14
  *        需求五：
  *        实时数据分析：  广告点击量实时统计
  */
object Req5AdsCLickCountApplication {
    def main(args: Array[String]): Unit = {

        // 需求五

        val sparkConf: SparkConf = new SparkConf().setAppName("Req4BlackNameListApplication").setMaster("local[*]")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

//        streamingContext.sparkContext.setCheckpointDir("cp")

        val topic = "ads_log"

        // TODO 从kafka中获取数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

        // TODO 将获取的kafka数据转换结构
        val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map(data => {
            val datas: Array[String] = data.value().split(" ")

            AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })



        // TODO 1.将数据转换结构(date_area_city_user , 1)
        val dateAdsUserToOneDStream: DStream[(String, Long)] = adsClickDStream.map(message => {
            val date = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
        })

        // TODO 2.将转换结构后的数据进行有状态聚合(date_area_city_user, sum)
        val reduceDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.reduceByKey(_+_)

       // TODO 3.更新redis中的统计结果
        reduceDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(dates=>{
                val client: Jedis = MyRedisUtil.getJedisClient

                dates.foreach{
                    case ( filed , sum) =>{
                        client.hincrBy("date:area:city:ads",filed,sum)
                    }
                }
                client.close()
            })
        })
        // 启动采集器
        streamingContext.start()

        // Driver应该等待采集器的执行结束
        streamingContext.awaitTermination()
    }
}
