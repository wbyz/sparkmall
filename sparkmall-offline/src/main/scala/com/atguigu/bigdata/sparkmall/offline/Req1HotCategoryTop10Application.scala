package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/**
  * @author Witzel
  * @since 2019/7/15 9:52
  *        获取点击、下单和支付数量排名前 10 的品类
  *
  */
object Req1HotCategoryTop10Application {
    def main(args: Array[String]): Unit = {

        // 需求一：获取点击、下单和支付数量排名前 10 的品类

        // TODO 4.0	创建SparkSQL的环境对象
        val sparkConf: SparkConf = new SparkConf().setAppName("Req1HotCategoryTop10Application").setMaster("local[*]")

        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        // 对象spark的类名
        import spark.implicits._
        // TODO 4.1	从hive中获取满足条件的数据
        spark.sql("use "+ConfigUtil.getValueByKey("hive.database"))
        var sql = "select * from user_visit_action where 1=1 "
        //获取条件
//        ConfigUtil.getValueByKey("condition.params.json")
        val startDate :String = ConfigUtil.getValueByJsonKey("startDate")
        val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

        if (StringUtil.isNotEmpty(startDate)){
            sql = sql + " and date >= '" + startDate + "'"
        }
        if (StringUtil.isNotEmpty(endDate)) {
            sql = sql + " and date <= '" + endDate + "'"
        }

        val actionDF: DataFrame = spark.sql(sql)

        val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
        val actionRDD: RDD[UserVisitAction] = actionDS.rdd

//        println(actionDF.count())

       // TODO 4.2	使用累加器类假数据，进行数据的聚合(categoryid-click, 100)，(categoryid-order, 100),(categoryid-pay, 100)
        val accumulator = new CategoryAccumulator
        spark.sparkContext.register(accumulator,"Category")

        actionRDD.foreach{
            actionData => {
                if(actionData.click_category_id != -1){
                    accumulator.add(actionData.click_category_id + "-click")
                }else if (StringUtil.isNotEmpty(actionData.order_category_ids)){
                    // 1,2,3
                    val ids: Array[String] = actionData.order_category_ids.split(",")
                    for (id <- ids){
                        accumulator.add(id + "-order")
                    }
                }else if (StringUtil.isNotEmpty(actionData.pay_category_ids)){
                    val ids: Array[String] = actionData.pay_category_ids.split(",")
                    for (id <- ids){
                        accumulator.add(id + "-pay")
                    }
                }
            }
        }
//        println(accumulator.value)
        // (category-指标，sumcount)
        val accuData: mutable.HashMap[String,Long] = accumulator.value
        // TODO 4.3	将累加器的结果通过品类ID进行分组(categoryid,[(order,100),(click,100),(pay,100)])
        val categoryTOAccuData: Map[String, mutable.HashMap[String, Long]] = accuData.groupBy{
            case (key, sumcount) => {
                val keys: Array[String] = key.split("-")
                keys(0)
            }
        }
        // TODO 4.4	将分组后的结果转换为对象CategoryTop10(categoryid,click,order,pay)
        val taskId = UUID.randomUUID().toString

        val categoryTop10Datas: immutable.Iterable[CategoryTop10] = categoryTOAccuData.map {
            case (categoryId, map) => {
                CategoryTop10(
                    taskId,
                    categoryId,
                    map.getOrElse(categoryId + "-click", 0L),
                    map.getOrElse(categoryId + "-order", 0L),
                    map.getOrElse(categoryId + "-pay", 0L)
                )
            }
        }
//        categoryTop10Datas.foreach(println)

        // TODO 4.5	将转换后的对象进行排序（点击，下单，支付）（降序）
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

//        println(sortList)

        // TODO 4.6 将排序后的结果取前10名保存到Mysql数据库中
        val top10List: List[CategoryTop10] = sortList.take(10)
        //println(top10List)

        val drvierClass = ConfigUtil.getValueByKey("jdbc.driver.class")
        val url = ConfigUtil.getValueByKey("jdbc.url")
        val user = ConfigUtil.getValueByKey("jdbc.user")
        val password = ConfigUtil.getValueByKey("jdbc.password")

        Class.forName(drvierClass)
        val connection: Connection = DriverManager.getConnection(url, user, password)

        val insertSQL = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values ( ?, ?, ?, ?, ? )"

        val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

        top10List.foreach{
            data => {
                pstat.setString(1, data.taskId)
                pstat.setString(2, data.categoryId)
                pstat.setLong(3, data.clickCount)
                pstat.setLong(4, data.orderCount)
                pstat.setLong(5, data.payCount)
                pstat.executeUpdate()
            }
        }

        pstat.close()
        connection.close()

        // TODO 4.7   释放资源
        spark.stop()


    }
}
// 样例类
case class CategoryTop10( taskId:String, categoryId:String, clickCount:Long, orderCount:Long, payCount:Long)

// 声明累加器，用于聚合相同品类的不同指标数据
// (categoryid-click,100),(categoryid-order,100)，(categoryid-pay,100)
class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String,Long]]{

    var map = new mutable.HashMap[String,Long]()

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
        map(v) = map.getOrElse(v, 0L) + 1
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
        // 两个map的合并
        val map1 = map
        val map2 = other.value
        map = map1.foldLeft(map2){
            (innerMap, t) => {
                innerMap(t._1) = innerMap.getOrElse(t._1,0L) + t._2
                innerMap
            }
        }
    }

    override def value: mutable.HashMap[String, Long] = {
        map
    }
}