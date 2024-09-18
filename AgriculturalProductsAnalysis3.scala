package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis2.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计山东省售卖蛤蜊的农产品市场占全省农产品市场的比例
 *
 */
object AgriculturalProductsAnalysis3 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {

    val RDD1: RDD[(String, String, String)] =
      productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(3), product(0)) //
      })
      .distinct()
      .filter(_._1 == "山东") //(山东,山东威海市农副产品批发市场,橙)

    val allCount = RDD1.count() //总数
    val geliCount = RDD1.filter(_._3 == "蛤蜊").count()
    println(allCount)
    println(geliCount)
    val bili = (geliCount.toFloat / allCount)
    println(bili)
      // RDD1.foreach(println)

        //保存数据到数据库
        val arrs = Array(allCount.toString,geliCount.toString,bili.toString)
        saveResult(arrs)

    sc.stop() //停止进程
  }


  //将数据存入数据库的方法
  def saveResult(result: Array[String]) {
    // 1、获取连接
    val connection: Connection = DriverManager
      .getConnection("jdbc:mysql://192.168.159.129:3306/Agricultural?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
        "root", "root")

    // 2、定义插入数据的sql语句
    val sql = "insert into AgriculturalProductsAnalysis3(allMarket,geliMarket,proportion) values(?,?,?)"

    // 3、获取PreParedStatement
    try {
      val ps: PreparedStatement = connection.prepareStatement(sql)

      // 4、获取数据，给？赋值
      ps.setString(1, result(0))
      ps.setString(2, result(1))
      ps.setString(3, result(2))
      // 5、执行
      ps.execute()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
