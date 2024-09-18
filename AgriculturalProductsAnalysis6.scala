package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis2.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 根据农产品类型数量，统计排名前5名的省份
 */
object AgriculturalProductsAnalysis6 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(0)) // (省份 农产品)
      })
      .distinct()   //去重
      .groupBy(_._1) //分组  (青海,CompactBuffer((青海,粳米), (青海,面粉), (青海,玉米)))
      .map(item => {
        (item._1, item._2.size, item._2) // (青海, 3, CompactBuffer((青海,粳米), (青海,面粉), (青海,玉米)))
      })
      .sortBy(_._2, false).take(5)   //根据农产品类型数量进行排序，并获取前5个
//      .foreach(item => {
//        println(item._1)
//      })


      .foreach(x => {

        //保存数据到数据库
        val arrs = Array(x._1.toString)
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
    val sql = "insert into AgriculturalProductsAnalysis6(province) values(?)"

    // 3、获取PreParedStatement
    try {
      val ps: PreparedStatement = connection.prepareStatement(sql)

      // 4、获取数据，给？赋值
      ps.setString(1, result(0))
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
