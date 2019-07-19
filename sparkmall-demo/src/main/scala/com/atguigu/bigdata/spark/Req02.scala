package com.atguigu.bigdata.spark

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
  * @since 2019/7/15 21:07
  *        需求二：
  *        Top10 热门品类中 Top10 活跃 Session 统计
  */
object Req02 {
    def main(args: Array[String]): Unit = {
        // TODO 4.0   创建SparkSQL的环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1")
        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

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

        //        println(actionDF.count())

        // TODO 4.2	使用累加器类假数据，进行数据的聚合(categoryid-click, 100)，(categoryid-order, 100),(categoryid-pay, 100)
        // 使用累加器：1、创建累加器对象 2、注册累加器(累加器对象，"自定义累加器名称")
        val accumulator = new CategoryAccumulator
        // 2、注册累加器(累加器对象，"自定义累加器名称")
        spark.sparkContext.register(accumulator, "Category")
        // 3、循环遍历数据，将每一条数据假如累加器，遍历的数据不需要返回值
        actionRDD.foreach {
            actionData => {
                if (actionData.click_category_id != -1) {
                    accumulator.add(actionData.click_category_id + "-click")
                } else if (StringUtil.isNotEmpty(actionData.order_category_ids)) {
                    // 1,2,3 可能有多个品类，所以需要先分隔字符串，在通过增强for，一个个累加数据
                    val ids: Array[String] = actionData.order_category_ids.split(",")
                    for (id <- ids) {
                        accumulator.add(id + "-order")
                    }
                } else if (StringUtil.isNotEmpty(actionData.pay_category_ids)) {
                    // 1,2,3 可能有多个品类
                    val ids: Array[String] = actionData.pay_category_ids.split(",")
                    for (id <- ids) {
                        accumulator.add(id + "-pay")
                    }
                }
                // 还有个search字段，和以上三种都互斥，所以以上都写else if，此时不考虑search，便暂时不写关于search判断内容
            }
        }
        //        println(accumulator.value)
        // 累加器结果结构为 (categoryId-指标 ，sumcount)
        val accuData: mutable.HashMap[String, Long] = accumulator.value

        // TODO 4.3	将累加器的结果通过品类ID进行分组(categoryId,[(order,100),(click,100),(pay,100)])
        // 按品类ID分组 (categoryId-指标 ，sumcount) ->(categoryId,[(categoryId-order,100),(categoryId-click,100),(categoryId-pay,100)])
        val categoryToAccuData: Map[String, mutable.HashMap[String, Long]] = accuData.groupBy {
            case (key, sumcount) => {
                val keys: Array[String] = key.split("-")
                keys(0)
            }
        }

        // TODO 4.4	将分组后的结果转换为对象CategoryTop10(categoryId,click,order,pay)
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

        // TODO 4.5	将转换后的对象进行排序（点击，下单，支付）（降序）
        val sortLsit: List[CategoryTop10] = categoryTop10Datas.toList.sortWith {
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

        // TODO 4.6	将排序后的结果去前10名
        val top10List: List[CategoryTop10] = sortLsit.take(10)

        /** ***********************************需求二代码 **************************************************/
        val ids: List[String] = top10List.map(_.categoryId)

        // TODO 4.1	将数据进行过来筛选，留下满足条件数据（点击数据，品类前10）
        val idsBroadcast: Broadcast[List[String]] = spark.sparkContext.broadcast(ids)

        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
            if (action.click_category_id != -1) {
                idsBroadcast.value.contains(action.click_category_id)
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
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
            datas.toList.sortWith {
                (left, right) => {
                    left._2 > right._2
                }
            }.take(10)
        })
        val mapRDD: RDD[List[CategorySessionTop10]] = resultRDD.map {
            case (categoryId, list) => {
                list.map {
                    case (sessionid, sum) => {
                        CategorySessionTop10(taskId, categoryId, sessionid, sum)
                    }
                }
            }
        }

        val dataRDD: RDD[CategorySessionTop10] = mapRDD.flatMap(list=>list)
        // TODO 4.7	将结果保存到Mysql中

        dataRDD.foreachPartition(datas=>{
            val drvierClass = ConfigUtil.getValueByKey("jdbc.driver.class")
            val url = ConfigUtil.getValueByKey("jdbc.url")
            val user = ConfigUtil.getValueByKey("jdbc.user")
            val password = ConfigUtil.getValueByKey("jdbc.password")

            Class.forName(drvierClass)
            val connection: Connection = DriverManager.getConnection(url, user, password)

            val insertSQL = "insert into category_top10_session_count ( taskId, categoryId, sessionId, clickCount ) values ( ?, ?, ?, ? )"

            val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

            datas.foreach(data=>{
                pstat.setString(1, data.taskId)
                pstat.setString(2, data.categoryId)
                pstat.setString(3, data.sessionId)
                pstat.setLong(4, data.clickCount)
                pstat.executeUpdate()
            })

            pstat.close()
            connection.close()

        })

        // TODO 4.7 释放资源
        spark.stop()
    }
}
case class CategorySessionTop10(taskId:String , categoryId:String , sessionId:String, clickCount:Int)
