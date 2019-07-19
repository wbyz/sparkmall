package com.atguigu.bigdata.spark

import java.util

import com.atguigu.bigdata.spark.util.{MyKafkaUtil, MyRedisUtil}
import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author Witzel
  * @since 2019/7/16 19:22
  */
object Req04 {
    def main(args: Array[String]): Unit = {

        // 需求四：广告黑名单实时统计

        // 准备SparkStreaming上下文环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApplication")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        streamingContext.sparkContext.setCheckpointDir("cp")

        val topic =  "ads_log"
        // TODO 从Kafka中获取数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

        // 将获取的kafka数据转换结构
        val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map(data => {

            val datas: Array[String] = data.value().split(" ")

            AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })

        /*
        adsClickDStream.foreachRDD{
            rdd => {
                rdd.foreach(println)
            }
        }
        */

        // TODO 0. 对数据进行筛选过滤，黑名单数据不需要
        /**
                //Driver
                /********此时还在Driver中内存中，是存在的************/
                val jedisClient: Jedis = MyRedisUtil.getJedisClient
                val userIds: java.util.Set[String] = jedisClient.smembers("blacklist")

                // 问题1 ： 会发生空指针异常，是因为序列化规则
                // BinaryJedis中的 list 前的关键字transient，表示可以绕过java序列化，是一个瞬时对象，不会序列化
                val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.filter(message => {
                    // Executor ，但此时在Executor中，因为没序列化，所以对象并没有传过来，所以对象为空
                    !userIds.contains(message.userid)
                })

                // 空指针异常： 1） 当调用一个为空对象的成员属性或者成员方法时会产出空指针异常
                //              2） 当进行自动拆箱时会出现空指针异常
                //              3） 当进行增强for时会出现空指针异常
                //              4）。。。。

                // 改进一： 使用广播变量
                // 问题2 ：黑名单数据无法更新，应该周期性的获取最新黑名单数据
                val userIdsBroadcast : Broadcast[java.util.Set[String] ]= streamingContext.sparkContext.broadcast(userIds)

                val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.filter(message => {
                  !useridsBroadcast.value.contains(message.userid)
                })
          */


        // 改进二：使用transform,可以多次执行driver上命令，
        // Driver(1)
         val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.transform(rdd => {
            // Drvier(N)
            val jedisClient: Jedis = MyRedisUtil.getJedisClient
            val userids: java.util.Set[String] = jedisClient.smembers("blacklist")
            jedisClient.close()
            // 使用广播变量
            val useridsBroadcast: Broadcast[java.util.Set[String]] = streamingContext.sparkContext.broadcast(userids)
            rdd.filter(message => {
                // Executor(M)
                !useridsBroadcast.value.contains(message.userid)
            })
         })

        // TODO 1. 将数据转换结构 （date-ads-user, 1）
        val dateAdsUserToOneDStream: DStream[(String, Long)] = filterDStream.map(message => {
            val date = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.adid + "_" + message.userid, 1L)
        })
        // TODO 2. 将转换结构后的数据进行有状态聚合 （date-ads-user, sum）
        val stateDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
            (seq: Seq[Long], buffer: Option[Long]) => {
                val sum = buffer.getOrElse(0L) + seq.size
                Option(sum)
            }
        }

        // TODO 3.对聚合后的记过进行阈值的判断
        /** ******************************redis （value的）五大数据类型 ********************************/
        // string（字符串），hash（哈希），list（列表），set（集合），zset(sorted set：有序集合)。
        // redis 五大数据类型 set
        // jedis
        stateDStream.foreachRDD(rdd=>{
            rdd.foreach{
                case (key , sum) => {
                    if(sum >=100){
                        // TODO 4. 如果超出阈值，将用户拉入黑名单
                        val keys: Array[String] = key.split("_")
                        val userId = keys(2)
                        // jedis
                        val client: Jedis = MyRedisUtil.getJedisClient
                        client.sadd("blacklist",userId)
                        // 关闭
                        client.close()
                    }
                }
            }
        })

        // 启动采集器
        streamingContext.start()
        // Driver应该等待采集器的执行结束
        streamingContext.awaitTermination()
    }
}
case class AdsClickKafkaMessage(timestamp: String, area: String, city: String, userid: String, adid: String)
