package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis1.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 计算山西省的每种农产品的价格波动趋势，即计算价格均值。
 *
 */
object AgriculturalProductsAnalysis4 {
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    val RDD1: RDD[(String, Double)] = productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(0), product(1).toDouble) // (省 农产品 价格)
      }).filter(_._1.equals("山西"))
      .map(item => item._2 -> item._3) // 构成元组((香菜,2.80))

    RDD1
      .groupByKey()     //key 表示是以元组第一个（就是key）来进行分组的
      .map(t => {      //计算出平均值
        if (t._2.size > 2)
          (t._1, ((t._2.sum - t._2.max - t._2.min) / t._2.size).formatted("%.2f"))
        else
          (t._1, (t._2.sum / t._2.size).formatted("%.2f"))
      }) // 农产品分类
      //.foreach(println)

      .foreach(x => {
        //保存数据到数据库
        val arrs = Array(x._1.toString,x._2.toString)
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
    val sql = "insert into AgriculturalProductsAnalysis4(species,average) values(?,?)"

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


//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//
//import java.sql.{Connection, DriverManager, PreparedStatement}
//
//object AvgPrice_Shanxi {
//
//  /**
//   * 计算山西省的每种农产品的价格波动趋势，即计算价格均值。
//       某种农产品的价格均值计算公式：PAVG = (PM1+PM2+...+PMn-max(P)-min(P))/(N-2)，
//       其中，P表示价格，Mn表示market，即农产品市场.
//       PM1表示M1农产品市场的该产品价格，max(P)表示价格最大值，min(P)价格最小值。
//   * @param args
//   */
//  def main(args: Array[String]): Unit = {
//
//    // 构建sparkConf对象 设置application名称和master地址
//    val conf: SparkConf = new SparkConf().setAppName("AvgPrice_Shanxi").setMaster("local[2]")
//    // 构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
//    val sc = new SparkContext(conf)
//    // 设置日志输出级别
//    sc.setLogLevel("WARN")
//
//    // 读取数据文件
//    val data: RDD[String] = sc.textFile("hdfs://192.168.44.155:9000/projectdatas/product.txt")
//    // 数据清洗，清除非法数据
//    val tmp = data.distinct().filter(x=>x.split("\t").length == 6)
//
//    val shanxi = tmp.filter(x=>x.split("\t")(4) == "山西")
//      .map(x => {
//        val data = x.split("\t")
//        ((data(4),data(0)),data(1).toDouble)  // 其对应的市场价格就是每一行的价格
//      })
//    val product = shanxi.groupByKey()
//    val avgPrice = product.map(x => {
//      val sum = x._2.reduce(_ + _)
//      val max = x._2.max
//      val min = x._2.min
//      val size = x._2.size
//      if (size > 2){
//        val avg = ((sum - max - min) / (size - 2)).formatted("%.2f")
//        (x._1._1, x._1._2, avg.toDouble)
//      } else if (size == 2){
//        (x._1._1, x._1._2, sum/2)
//      } else {
//        (x._1._1, x._1._2, sum)
//      }
//
//    })
//    product.foreach(println(_))
//    println("============================")
//    avgPrice.foreach(x => {
//      println(x._1 + "省的 " + x._2 + " 产品的价格均值是 " + x._3 + " 元")
//      val arrs = Array(x._1, x._2, x._3.toString)
//      saveResult(arrs)
//    })
//
//
//  }
//
//  /**
//   * 将处理后的数据保存至 MySQL 数据库
//   * @param result
//   */
//  def saveResult(result: Array[String]) {
//    // 1、获取连接
//    val connection: Connection = DriverManager
//      .getConnection("jdbc:mysql://192.168.44.155:3306/products?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
//        "root","root")
//
//    // 2、定义插入数据的sql语句
//    val sql = "insert into productsAvgPrice_Shanxi(province,product,avgPrice) values(?,?,?)"
//
//    // 3、获取PreParedStatement
//    try {
//      val ps: PreparedStatement = connection.prepareStatement(sql)
//
//      // 4、获取数据，给？赋值
//      ps.setString(1, result(0))
//      ps.setString(2, result(1))
//      ps.setString(3, result(2))
//      // 5、执行
//      ps.execute()
//    } catch {
//      case e:Exception => e.printStackTrace()
//    } finally {
//      if (connection != null) {
//        connection.close()
//      }
//    }
//  }
//
//}

