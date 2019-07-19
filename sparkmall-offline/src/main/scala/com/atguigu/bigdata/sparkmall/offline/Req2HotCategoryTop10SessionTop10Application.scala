package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.{immutable, mutable}

/**
  * @author Witzel
  * @since 2019/7/15 15:33
  *        Top10 热门品类中 Top10 活跃 Session 统计
  */
object Req2HotCategoryTop10SessionTop10Application {
    def main(args: Array[String]): Unit = {
        // 需求二：Top10 热门品类中 Top10 活跃 Session 统计

        // TODO 4.0	配置SparkSQL环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req2HotCategoryTop10SessionTop10Application")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        // *******************************************需求一代码***************************************************
        import spark.implicits._

        // TODO 4.1 从Hive中获取满足条件的数据
        spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
        var sql = " select * from user_visit_action where 1 = 1 "

        // 获取条件
        //val jsonString = ConfigUtil.getValueByKey("condition.params.json")
        val startDate: String = ConfigUtil.getValueByJsonKey("startDate")
        val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

        if ( StringUtil.isNotEmpty(startDate) ) {
            sql = sql + " and date >= '" + startDate + "' "
        }

        if ( StringUtil.isNotEmpty(endDate) ) {
            sql = sql + " and date <= '" + endDate + "' "
        }

        val actionDF: DataFrame = spark.sql(sql)
        val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
        val actionRDD: RDD[UserVisitAction] = actionDS.rdd

        //println(actionDF.count())

        // TODO 4.2 使用累加器累加数据，进行数据的聚合
        // ( categoryid-click,100  ), ( categoryid-order,100  ), ( categoryid-pay,100  )
        val accumulator = new CategoryAccumulator
        spark.sparkContext.register(accumulator, "Category")

        actionRDD.foreach{
            actionData => {
                if ( actionData.click_category_id != -1 ) {
                    accumulator.add(actionData.click_category_id + "-click")
                } else if ( StringUtil.isNotEmpty(actionData.order_category_ids) ) {
                    // 1,2,3
                    val ids: Array[String] = actionData.order_category_ids.split(",")
                    for ( id <- ids ) {
                        accumulator.add(id + "-order")
                    }
                } else if ( StringUtil.isNotEmpty(actionData.pay_category_ids) ) {
                    val ids: Array[String] = actionData.pay_category_ids.split(",")
                    for ( id <- ids ) {
                        accumulator.add(id + "-pay")
                    }
                }
            }
        }

        //println(accumulator.value)
        // (category-指标，sumcount)
        val accuData: mutable.HashMap[String, Long] = accumulator.value

        // TODO 4.3 将累加器的结果通过品类ID进行分组（ categoryid，[（order,100）,(click:100), (pay:100)] ）
        val categoryToAccuData: Map[String, mutable.HashMap[String, Long]] = accuData.groupBy {
            case (key, sumcount) => {
                val keys: Array[String] = key.split("-")
                keys(0)
            }
        }

        // TODO 4.4 将分组后的结果转换为对象CategoryTop10(categoryid, click,order,pay )
        val taskId = UUID.randomUUID().toString

        val categoryTop10Datas: immutable.Iterable[CategoryTop10] = categoryToAccuData.map {
            case (categoryId, map) => {
                CategoryTop10(
                    taskId,
                    categoryId,
                    map.getOrElse(categoryId + "-click", 0L),
                    map.getOrElse(categoryId + "-order", 0L),
                    map.getOrElse(categoryId + "-pay", 0L))
            }
        }


        // TODO 4.5 将转换后的对象进行排序（点击，下单， 支付）（降序）
        val sortList: List[CategoryTop10] = categoryTop10Datas.toList.sortWith {
            (left, right) => {
                if (left.clickCount > right.clickCount) {
                    true
                } else if (left.clickCount == right.clickCount) {
                    if (left.orderCount > right.orderCount) {
                        true
                    } else if (left.orderCount == right.orderCount) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }

        // TODO 4.6 将排序后的结果取前10名保存到Mysql数据库中
        val top10List: List[CategoryTop10] = sortList.take(10)
        val ids: List[String] = top10List.map(_.categoryId)
        // *******************************************需求二代码***************************************************
        // TODO 4.1	将数据进行过来筛选，留下满足条件数据（点击数据，品类前10）

        val idsBroadcast: Broadcast[List[String]] = spark.sparkContext.broadcast(ids)

        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
            if (action.click_category_id != -1) {
                ids.contains(action.click_category_id)
            } else {
                false
            }
        })

        // TODO 4.2	将过滤后的数据进行结构的转换（categoryId+sessionId,1）
        val categoryIdAndSessionToOneRDD: RDD[(String, Int)] = filterRDD.map(action => {
            (action.click_category_id + "_" + action.session_id, 1)
        })

        // TODO 4.3	将转换结构后的数据进行聚合统计（categoryId+sessionId,sum）
        val categoryIdAndSessionToSumRDD: RDD[(String, Int)] = categoryIdAndSessionToOneRDD.reduceByKey(_+_)

        // TODO 4.4	将聚合后的结果数据进行结构的转换（categoryId+sessionId,sum）->（categoryId,（sessionId,sum））
        val categoryToSessionAndSumRDD: RDD[(String, (String, Int))] = categoryIdAndSessionToSumRDD.map {
            case (key, sum) => {
                val keys: Array[String] = key.split("_")
                (keys(0), (keys(1), sum))
            }
        }
        // TODO 4.5	将转换结构后的数据进行分组（categoryId,Iterator[(sessionId,sum)]））
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = categoryToSessionAndSumRDD.groupByKey()

        // TODO 4.6	将分组后的数据进行排序取前10
        // 先按value大小排序
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
            datas.toList.sortWith {
                (left, right) => {
                    left._2 > right._2
                }
            }.take(10)
        })

        // 再转换数据结构
        val mapRDD: RDD[List[CategorySessionTop10]] = resultRDD.map {
            case (categoryId, list) => {
                list.map {
                    case (sessionid, sum) => {
                        CategorySessionTop10(taskId, categoryId, sessionid, sum)
                    }
                }
            }
        }

        // 扁平化成一个个元素
        val dataRDD: RDD[CategorySessionTop10] = mapRDD.flatMap(list=>list)

        // TODO 4.7	将结果保存到Mysql中
        dataRDD.foreachPartition(datas =>{
            val drvierClass = ConfigUtil.getValueByKey("jdbc.driver.class")
            val url = ConfigUtil.getValueByKey("jdbc.url")
            val user = ConfigUtil.getValueByKey("jdbc.user")
            val password = ConfigUtil.getValueByKey("jdbc.password")

            Class.forName(drvierClass)
            val connection: Connection = DriverManager.getConnection(url, user, password)

            val insertSQL = "insert into category_top10_session_count ( taskId, categoryId, sessionId, clickCount ) values ( ?, ?, ?, ? )"

            val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

            datas.foreach(data => {
                pstat.setString(1, data.taskId)
                pstat.setString(2, data.categoryId)
                pstat.setString(3, data.sessionId)
                pstat.setLong(4, data.clickCount)
                pstat.executeUpdate()
            })

            pstat.close()
            connection.close()
        })

        // TODO 释放资源
        spark.stop()
/*
        dataRDD.foreach{
            data => {
                val drvierClass = ConfigUtil.getValueByKey("jdbc.driver.class")
                val url = ConfigUtil.getValueByKey("jdbc.url")
                val user = ConfigUtil.getValueByKey("jdbc.user")
                val password = ConfigUtil.getValueByKey("jdbc.password")

                Class.forName(drvierClass)
                val connection: Connection = DriverManager.getConnection(url, user, password)

                val insertSQL = "insert into category_top10_session_count ( taskId, categoryId, sessionId, clickCount ) values ( ?, ?, ?, ? )"

                val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

                pstat.setString(1, data.taskId)
                pstat.setString(2, data.categoryId)
                pstat.setLong(3, data.sessionId)
                pstat.setLong(4, data.clickCount)
                pstat.executeUpdate()
                pstat.close()
                connection.close()
            }
        }
*/

    }
}

case class CategorySessionTop10(taskId:String , categoryId:String , sessionId:String, clickCount:Int)

