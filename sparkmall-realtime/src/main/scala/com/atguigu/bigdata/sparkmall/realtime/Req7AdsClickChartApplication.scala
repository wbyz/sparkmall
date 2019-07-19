package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

/**
  * @author Witzel
  * @since 2019/7/17 14:09
  *        需求七：
  *        最近一分钟广告点击趋势（每10秒）
  */
object Req7AdsClickChartApplication {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Req4BlackNameListApplication").setMaster("local[*]")

        // 采集周期是5秒
        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        val topic = "ads_log"

        // TODO 从kafka中获取数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

        // TODO 将获取的kafka数据转换结构
        val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map(data => {
            val datas: Array[String] = data.value().split(" ")

            AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })

        // TODO 1.使用窗口函数将数据进行封装 （windowDuration：窗口大小，slideDuration：步长）
        // 都得是采集周期的整数倍，采集周期为5s，所以窗口大小设为1分钟，步长（滑动幅度）为10秒
        val windowDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.window(Seconds(60), Seconds(10))

        // TODO 2.将数据进行结构的转换(15:11 ==> 15:10)
        val timeToOneDStream: DStream[(String,Long)] = windowDStream.map(message => {
            val timeString: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong)

            // substring（beginIndex,endIndex） 从beginIndex开始到endIndex前一位结束。 所以不包含endIndex
            val time: String = timeString.substring(0, timeString.length - 1)+"0"

            (time,1L)
        })

        // TODO 3.将转换结构后的数据进行聚合统计
        val timeToSumDStream: DStream[(String, Long)] = timeToOneDStream.reduceByKey(_+_)

        // TODO 4.对统计结果进行排 (2019-07-18 17:17:30,130)
        val sortDStream: DStream[(String, Long)] = timeToSumDStream.transform(rdd => {
            rdd.sortByKey()
        })

        sortDStream.print()

        // 启动采集器
        streamingContext.start()
        //
        streamingContext.awaitTermination()

    }
}













