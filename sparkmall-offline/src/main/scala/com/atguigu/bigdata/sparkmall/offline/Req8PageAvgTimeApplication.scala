package com.atguigu.bigdata.sparkmall.offline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author Witzel
  * @since 2019/7/16 10:25
  *        需求八：
  *        每个页面用户的平均停留时间
  */
object Req8PageAvgTimeApplication {
    def main(args: Array[String]): Unit = {
        // TODO 4.0   创建SparkSQL的环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1")
        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        spark.sparkContext.setCheckpointDir("cp")

        import spark.implicits._
        // TODO 4.1	从hive中获取满足条件的数据
        spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
        var sql = "select * from user_visit_action where 1=1 "

        // 获取条件 json字符串
        //        val jsonString: String = ConfigUtil.getValueByKey("condition.params.json")
        val startDate: String = ConfigUtil.getValueByJsonKey("startDate")
        val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

        if (StringUtil.isNotEmpty(startDate)) {
            sql = sql + "and date >= '" + startDate + "' "
        }
        if (StringUtil.isNotEmpty(endDate)) {
            sql = sql + "and date <= '" + endDate + "' "
        }

        val actionDF: DataFrame = spark.sql(sql)
        val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
        val actionRDD: RDD[UserVisitAction] = actionDS.rdd


        // 使用检查点缓存数据
        actionRDD.checkpoint()
        /****************************************需求八代码*****************************************/

        // TODO 1.将数据根据session进行分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

        // TODO 2.将分组后的数据进行时间排序（升序）
        val sessionToPageidAndTimeXRDD: RDD[(String, List[(Long, Long)])] = groupRDD.mapValues(datas => {
            val sortList: List[UserVisitAction] = datas.toList.sortWith {
                (left, right) => {
                    left.action_time < right.action_time
                }
            }
            // TODO 3.将页面数据进行拉链（1-2，time2-time1）
            val idToTimeList: List[(Long, String)] = sortList.map(action => {
                (action.page_id, action.action_time)
            })

            // ((pageid1,time1),(pageid2,time2))
            val pageid1ToPageid2List: List[((Long, String), (Long, String))] = idToTimeList.zip(idToTimeList.tail)

            // TODO 4.将拉链数据进行结构转变(（1，timeX）,（1，timeX），（1，timeX）)
            pageid1ToPageid2List.map {
                case (page1, page2) => {
                    val time1 = DateUtil.parseLongByString(page1._2)
                    val time2 = DateUtil.parseLongByString(page2._2)

                    val timeX = time2 - time1

                    (page1._1, timeX)
                }
            }
        })

        // TODO 5.将转变结构后的数据进行分组（pageid，Terator[（time）]）
        val pageidToTimeListXRDD: RDD[List[(Long, Long)]] = sessionToPageidAndTimeXRDD.map {
            case (k, v) => v
        }
        val pageidToTiemXRDD: RDD[(Long, Long)] = pageidToTimeListXRDD.flatMap(list=>list)

        val pageidGroupRDD: RDD[(Long, Iterable[Long])] = pageidToTiemXRDD.groupByKey()

        // TODO 6.获取最终结果（pageid,timeSum/timeSize）
        pageidGroupRDD.foreach {
            case (pageid, timeX) => {
                println("页面" + pageid + "平均停留时间" + (timeX.sum / timeX.size))
            }
        }

        // TODO 释放资源
        spark.close()

    }
}
