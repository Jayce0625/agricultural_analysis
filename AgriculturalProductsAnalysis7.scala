package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis1.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 东北三省农产品最高价格降序排列，统计前十名的农产品有哪些 (黑龙江 吉林 辽宁)
 */
object AgriculturalProductsAnalysis7 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    productRDD.filter(_.split("\\s+").length == 6)  //筛除空白隔开的不是6部分的数据
      .map(line => {
        val product = line.split("\\s+")    //将每一行数据中使用空白分割开
        (product(4), product(0), product(1).toDouble) // (山西 农产品类型)
      })
      .distinct()
      //筛选出省份是东北三省的数据
      .filter(item => {
        item._1.equals("黑龙江") || item._1.equals("吉林") || item._1.equals("辽宁")
      })
      .sortBy(_._3, false).take(10)   //加上false是进行逆向排序
      //.foreach(println)

      .foreach(x => {
        //保存数据到数据库
        val arrs = Array(x._1.toString,x._2.toString,x._3.toString)
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
    val sql = "insert into AgriculturalProductsAnalysis7(province,species,price) values(?,?,?)"

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
