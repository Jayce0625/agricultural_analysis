package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis1.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计哪些农产品市场在售卖樱桃，要标明农产品市场所在省份与城市

 */
object AgriculturalProductsAnalysis2 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(5), product(3), product(0)) //
      })

      //直接过滤掉农产品（productRDD数据集中的第4个数据）不是樱桃的数据
      .filter(_._4 == "樱桃")
      .distinct()
      //.foreach(println(_))

      .foreach(x => {

        //保存数据到数据库
        val arrs = Array(x._1.toString,x._2.toString,x._3.toString,x._4.toString)
        saveResult(arrs)
      })
    sc.stop() //结束进程
  }



  //将数据存入数据库的方法
  def saveResult(result: Array[String]) {
    // 1、获取连接
    val connection: Connection = DriverManager
      .getConnection("jdbc:mysql://192.168.159.129:3306/Agricultural?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
        "root", "root")

    // 2、定义插入数据的sql语句
    val sql = "insert into AgriculturalProductsAnalysis2(province,city,market,species) values(?,?,?,?)"

    // 3、获取PreParedStatement
    try {
      val ps: PreparedStatement = connection.prepareStatement(sql)

      // 4、获取数据，给？赋值
      ps.setString(1, result(0))
      ps.setString(2, result(1))
      ps.setString(3, result(2))
      ps.setString(4, result(3))
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
