package com.atguigu.bigdata.spark

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author Witzel
  * @since 2019/7/16 11:39
  */
object Req03 {
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

        /*****************************************需求三代码********************************************/
        // TODO 4.1 计算分母的数据
        //      TODO 4.1.1 将原始数据进行结构的转换，用于统计(pageid , 1)
        // 先获得pageid集合
        val pageids: Array[String] = ConfigUtil.getValueByJsonKey("targetPageFlow").split(",")

        // 过滤，只要在pageids范围内的数据
        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
            // (分母集合,pageid只需要1-6,pageid = 7可以不要，所以用init方法只取末尾前所有的元素)
            pageids.init.contains(action.page_id.toString)
        })

        val pageMapRDD: RDD[(Long, Long)] = filterRDD.map(action => {
            (action.page_id, 1L)
        })

        //      TODO 4.1.2 将转换后的结果进行聚合统计(pageid,1) -> (pageid,sumcount)
        val pageidToSumRDD: RDD[(Long, Long)] = pageMapRDD.reduceByKey(_+_)
        
        // TODO 4.2 计算分子的数据
        //      TODO 4.2.1 将原始数据通过session进行分组(session,iterator[(pageid,action_time) ] )
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

        //      TODO 4.2.2 将分组后的数据使用时间进行排序(升序)
        // 只对value操作 value=（pageid,action_time）
        groupRDD.mapValues{
            datas => {
                // 排序只需要按时间戳升序来排 即value.action_time
                val actions: List[UserVisitAction] = datas.toList.sortWith {
                    (left, right) => {
                        left.action_time < right.action_time
                    }
                }
                // value值只取page_id
                // 1,2,3,4,5,6,7
                // 2,3,4,5,6,7
                val sessionPageIds: List[Long] = actions.map(_.page_id)
                val pageId1ToPageId2: List[(Long, Long)] = sessionPageIds.zip(sessionPageIds.tail)
            }
        }
        //      TODO 4.2.3 将排序后的数据进行拉链处理，形成单挑页面流转顺序
        //      TODO 4.2.4 将处理后的数据进行筛选过来，保留需要关心的流转数据
        //      TODO 4.2.5 对过来后的数据进行结构的转化(pageid1-pageid2, 1)
        //      TODO 4.2.6将转换结构后的数据进行聚合统计(pageid1-pageid2, sumcount)
        // TODO 4.3 使用分子数据除以分母数据(sumcount1 / sumcount)



        spark.close()
    }
}
