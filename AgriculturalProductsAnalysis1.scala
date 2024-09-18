package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis1.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计 统计每个省份的农产品种类总数

 */
object AgriculturalProductsAnalysis1 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(0)) //去除省份与农产品
      })
      .distinct()
      .groupBy(_._1) // 根据省份进行分组 (青海,CompactBuffer((青海,粳米), (青海,面粉), (青海,玉米)))
      .map(item => {
        (item._1, item._2.toBuffer.size)
      })
      //.foreach(println(_))
      .foreach(x => {
        //保存数据到数据库
        val arrs = Array(x._1.toString,x._2.toString) //将其转换为数组
        saveResult(arrs)
      })
    sc.stop()

  }



  //将数据存入数据库的方法
  def saveResult(result: Array[String]) {
    // 1、获取连接
    val connection: Connection = DriverManager
      .getConnection("jdbc:mysql://192.168.159.129:3306/Agricultural?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
        "root", "root")

    // 2、定义插入数据的sql语句
    val sql = "insert into AgriculturalProductsAnalysis1(province,speciesCount) values(?,?)"

    // 3、获取PreParedStatement
    try {
      val ps: PreparedStatement = connection.prepareStatement(sql)

      // 4、获取数据，给？赋值
      ps.setString(1, result(0))
      ps.setString(2, result(1))
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
