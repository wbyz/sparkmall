package com.atguigu.bigdata.sparkmall.offline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author Witzel
  * @since 2019/7/16 10:25
  *        需求三：
  *        页面单跳转化率统计
  */
object Req3PageFlowApplication {
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
        /****************************************需求三代码*****************************************/
        // TODO 4.1 计算分母的数据
        //      TODO 4.1.1 将原始数据进行结构的转换，用于统计(pageid , 1 )
        val pageids: Array[String] = ConfigUtil.getValueByJsonKey("targetPageFlow").split(",")

        // (1-2) ,( 2-3) , (3-4) .......
        val pageflowIds: Array[String] = pageids.zip(pageids.tail).map {
            case (pageid1, pageid2) => {
                pageid1 + "-" + pageid2
            }
        }

        //(1->2)/1 ......  (6->7)/6 // 所以可以过滤7,支取1-6的访问记录
        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
            // init: 取最后一个元素之前的左右元素
            pageids.init.contains(action.page_id.toString)
        })

        val pageMapRDD: RDD[(Long, Long)] = filterRDD.map(action => {
            (action.page_id, 1L)
        })

        //      TODO 4.1.2 将转换后的结果进行聚合统计(pageid, 1) -> (pageid,sumcount)
        val pageIdToSumRDD: RDD[(Long, Long)] = pageMapRDD.reduceByKey(_+_)

        val pageIdToSumMap: Map[Long, Long] = pageIdToSumRDD.collect().toMap

        // TODO 4.2 计算分子的数据
        //      TODO 4.2.1 将原始数据通过session进行分组(session,iterator[(pageid,action_time) ] )
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

        //      TODO 4.2.2 将分组后的数据使用时间进行排序(升序)
        val sessionToPageflowRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues {
            datas => {
                val actions: List[UserVisitAction] = datas.toList.sortWith {
                    (left, rigth) => {
                        left.action_time < rigth.action_time
                    }
                }
                // 1,2,3,4,5,6,7
                // 2,3,4,5,6,7
        /** 考虑拉链时，scala集合和rdd的区别：
          *             两个scala集合长度若不一致，多余部分不会进行拉链，且会被丢弃
          *             rdd拉链时，需要考虑分区和分区内个数，所以长度必须相等，不相等不能进行拉链
          */
                // 拉链
                val sessionPageids: List[Long] = actions.map(_.page_id)
                // 1-2 , 2-3 , 3-4 , 4-5 , 5-6 , 6-7
        //      TODO 4.2.3 将排序后的数据进行拉链处理，形成单挑页面流转顺序
                val pageid1Topageid2: List[(Long, Long)] = sessionPageids.zip(sessionPageids.tail)
                pageid1Topageid2.map {
                    case (pageid1, pageid2) => {
        //      TODO 4.2.4 将处理后的数据进行筛选过来，保留需要关心的流转数据
                        (pageid1 + "-" + pageid2, 1L)
                    }
                }
            }
        }

        //  TODO 4.2.5 对过来后的数据进行结构的转化(pageid1-pageid2, 1)
        val mapRDD: RDD[List[(String, Long)]] = sessionToPageflowRDD.map(_._2)

        // (1-2,1)......
        val flatMapRDD: RDD[(String, Long)] = mapRDD.flatMap(list=>list)

        val finalPageflowRDD: RDD[(String, Long)] = flatMapRDD.filter {
            case (pageflow, one) => {
                pageflowIds.contains(pageflow)
            }
        }


        //      TODO 4.2.6将转换结构后的数据进行聚合统计(pageid1-pageid2, sumcount)
        val resultRDD: RDD[(String, Long)] = finalPageflowRDD.reduceByKey(_+_)

        // TODO 4.3 使用分子数据除以分母数据(sumcount1 / sumcount)
        resultRDD.foreach(println)
        resultRDD.foreach{
            case (pageflow , sum ) => {
                val ids: Array[String] = pageflow.split("-")
                val pageid = ids(0)
                val sum1 = pageIdToSumMap.getOrElse(pageid.toLong, 1L)

                println(pageflow + "=" + (sum.toDouble / sum1))
            }
        }


        // TODO 释放资源
        spark.close()

    }
}
