package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis2.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计排名前3的省份共同拥有的农产品类型
 */
object AgriculturalProductsAnalysis5 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    val productRDD1 = productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(0)) // (省份 农产品类型)
      })
      .distinct()   //去重，此处是保证每个省只有一种农产品
      .groupBy(_._1) //根据省份进行划分 (青海,CompactBuffer((青海,粳米), (青海,面粉), (青海,玉米)))
      .map(item => {
        (item._1, item._2.size, item._2) // (青海, 该省份农产品种类数量, CompactBuffer((青海,粳米), (青海,面粉), (青海,玉米)))
      })
      .sortBy(_._2, false).take(3)   //根据农产品种类的数量进行排序，take表示取前三个
      //.foreach(println)  //北京、江苏、山东

val productRDD2 = productRDD1
      .flatMap(item => {  //此处只选出前三个省份的农产品，并将农产品进行扁平化处理
        (item._3).map(item => (item._2))  //CompactBuffer((青海,粳米), (青海,面粉), (青海,玉米)).map(item => (农产品))
      })
      .map(item => item -> 1)     // 将每一个农产品都转化为元组的形式  (农产品，1)
      .groupBy(_._1) //根据农产品进行分组（农产品，（农产品，1））
      //筛选
      .filter(item => item._2.size == 3)   //表示此农产品每个省都有一个（前面已经去重），加起来就是3
      //  .foreach(println)


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
    val sql = "insert into AgriculturalProductsAnalysis5(species) values(?)"

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
