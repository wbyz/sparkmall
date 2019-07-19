package com.atguigu.bigdata.sparkmall.realtime

import com.atguigu.bigdata.sparkmall.common.util.DateUtil
import com.atguigu.bigdata.sparkmall.realtime.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author Witzel
  * @since 2019/7/16 14:14
  */
object Req4RedisBlackNameListApplication {
    def main(args: Array[String]): Unit = {

        // 需求四

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


        // TODO 0.对数据进行筛选过滤，过滤掉黑名单数据

        // 改进二：
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

        // TODO 1.将数据转换结构(date_ads_user , 1)
        val dateAdsUserToOneDStream: DStream[(String, Long)] = filterDStream.map(message => {
            val date = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.adid + "_" + message.userid, 1L)
        })

        // TODO 2.将转换结构后的数据进行redis聚合
        dateAdsUserToOneDStream.foreachRDD(rdd => {
            rdd.foreachPartition(datas=>{
                val client: Jedis = MyRedisUtil.getJedisClient
                val key = "date:ads:user:click"

                datas.foreach{
                    case (field,one) =>{
                        client.hincrBy(key,field,1L)

                        // TODO 3.对聚合后的记过进行阈值的判断
                        val sum: Long = client.hget(key,field).toLong
                        if(sum >= 100){
                            val keys: Array[String] = field.split("_")
                            val userid = keys(2)

                            // TODO 4. 如果超出阈值，将用户拉入黑名单
                            client.sadd("balcklist",userid)
                        }
                    }
                }

                client.close()
            })
            /*rdd.foreach{
                case (field,one) =>{
                    // 将数据在redis中聚合
                    // redis中格式（k-v） 用hash数据类型
                    val client: Jedis = MyRedisUtil.getJedisClient
                    val key = "date:ads:user:click"

                    client.hincrBy(key,field,1L)

                    // TODO 3.对聚合后的记过进行阈值的判断
                    val sum: Long = client.hget(key,field).toLong
                    if(sum >= 100){
                        val keys: Array[String] = field.split("_")
                        val userid = keys(2)

                        // TODO 4. 如果超出阈值，将用户拉入黑名单
                        client.sadd("balcklist",userid)
                    }

                    client.close()
                }
            }*/
        })

/*
        // TODO 3.对聚合后的记过进行阈值的判断
        //  ******************************redis （value的）五大数据类型 ********************************
        // string（字符串），hash（哈希），list（列表），set（集合），zset(sorted set：有序集合)。
        //jedis
//        val client: Jedis = MyRedisUtil.getJedisClient
        stateDStream.foreachRDD(rdd=>{
            rdd.foreach{
                case ( key, sum ) => {
                    if ( sum >= 100 ) {
                        // TODO 4. 如果超出阈值，将用户拉入黑名单
                        val keys: Array[String] = key.split("_")
                        val userid = keys(2)

                        val client: Jedis = MyRedisUtil.getJedisClient
                        client.sadd("blacklist", userid)
                        client.close()
                    }
                }
            }
        })
*/

        // 启动采集器
        streamingContext.start()

        // Driver应该等待采集器的执行结束
        streamingContext.awaitTermination()
    }
}



