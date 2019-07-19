package com.atguigu.bigdata.spark

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/**
  * @author Witzel
  * @since 2019/7/15 19:04
  *        需求一：
  *        获取点击、下单和支付数量排名前 10 的品类
  */
object Req1 {
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

        // TODO 4.6	将排序后的结果去前10名，保存到Mysql数据库中。
        val top10List: List[CategoryTop10] = sortLsit.take(10)
        //        println(top10List)

        val driverClass = ConfigUtil.getValueByKey("jdbc.driver.class")
        val url = ConfigUtil.getValueByKey("jdbc.url")
        val user = ConfigUtil.getValueByKey("jdbc.user")
        val password = ConfigUtil.getValueByKey("jdbc.password")

        Class.forName(driverClass)
        val connection: Connection = DriverManager.getConnection(url, user, password)

        val insertSQL = "insert into category_top10 (takeId,categoryId,clickCount,orderCount,payCount) values (?,?,?,?,?)"

        val pstate: PreparedStatement = connection.prepareStatement(insertSQL)

        top10List.foreach{
            data => {
                pstate.setString(1,data.taskId)
                pstate.setString(2,data.categoryId)
                pstate.setLong(3,data.clickCount)
                pstate.setLong(4,data.orderCount)
                pstate.setLong(5,data.payCount)
                pstate.executeUpdate()
            }
        }

        pstate.close()
        connection.close()

        // TODO 4.7	释放资源
        spark.stop()

    }
}

// 样例类
case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

// 声明累加器，用于聚合相同品类的不同指标数据

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

    var map = new mutable.HashMap[String, Long]()

    override def isZero: Boolean = {
        map.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
        new CategoryAccumulator
    }

    override def reset(): Unit = {
        map.clear()
    }

    override def add(v: String): Unit = {
        // v是传参，代表key值，方法计算是判断传参v之前是否已存在在map中，若有取出对应value值，若没有则默认为0，最后取出的值加1。得到本次累加结果
        map(v) = map.getOrElse(v, 0L) + 1
    }

    // 合并 多个Executor中的累加器最后合并到Driver上，每个累加器的返回结果是map，所以最后就是map的合并
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
        // 两个map的合并
        val map1 = map
        val map2 = other.value
        // 参数1 z:B 表示参数z是B类型的 ， 参数2 表示返回值是B类型的,且参数类是(B,(String,Long))。(String,Long)可以看做一个tuple，简写为t
        map = map1.foldLeft(map2) {
            (innerMap, t) => {
                // 先获取t中的key值，再判断该key值是否存在在innerMap中，若有取值，若没有默认为0。将取值和t的value相加，最后赋值给innerMap中key对应的value中
                innerMap(t._1) = innerMap.getOrElse(t._1, 0L) + t._2
                innerMap
            }
        }
    }

    // 最后输出
    override def value: mutable.HashMap[String, Long] = {
        map
    }
}