package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis1.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计数据中缺失农产品市场的省份有哪些
 */
object AgriculturalMarketAnalysis2 {

  //构建SparkConf，设置application和master地址、SparkContext对象是spark程序执行的入口
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")
  private val allProvinceRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/allprovince.txt")

  def main(args: Array[String]): Unit = {
    //按照一个或者多个tab键切割(\s匹配任何空白字符，包括空格、制表符、换页符等等)
    //使用filter过滤出使用空白隔开的，一行有6段数据的数据，将其放入弹性分布式数据集内
    val provinceRDD: RDD[String] = productRDD.filter(_.split("\\s+").length == 6)
     //使用map语句，筛选出product.txt文件内的所有省份
      .map(line => {
        val parts: Array[String] = line.split("\\s+")
        (parts(4))
      })

    //使用map筛选出allprovince.txt文件内所有的省份
    val allProvinceRDDs: RDD[String] = allProvinceRDD.map(line => line.split("\\s+")(0))
    //两者做差，筛选出product.txt中没有出现的省份
    val resRDD: RDD[String] = allProvinceRDDs.subtract(provinceRDD)
    resRDD.foreach(println)
    resRDD.foreach(x => {

      //保存数据到数据库
      val arrs = Array(x)
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
    val sql = "insert into AgriculturalMarketAnalysis2(province) values(?)"

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
