package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis3.{saveResult, sc}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计 山东 和 山西 两省售卖 土豆 的农产品市场总数
 */
object AgriculturalMarketAnalysis4 {

  //创建SparkConf对象，设置application与master地址
  //创建SparkContext对象，是所有spark程序的执行入口
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("dataframe"))
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    productRDD.filter(_.split("\\s+").length == 6)

      //使用map对productRDD中的所有数据使用空白进行分割，之后取出每一行分割的第5，4，1个数据
      .map(line => {
        val product = line.split("\\s+")
        (product(4), product(3), product(0))
      })

      //去除重复
      .distinct()
      //进行过滤
      .filter(item => {
        (item._1 == "山东" || item._1 == "山西") && item._3 == "土豆"
      })
    //按照山东进行分组 (山东,CompactBuffer((山东,山东章丘批发市场,土豆), (山东,山东中国寿光农产品物流园,土豆), (山东,山东青岛城阳蔬菜批发市场,土豆), (山东,山东青岛李沧区沧口蔬菜副食品,土豆), (山东,山东威海市农副产品批发市场,土豆), (山东,山东德州农业蔬菜局,土豆), (山东,山东青岛平度市南村蔬菜批发市场,土豆)))
      .groupBy(_._1)
      .map(item => {
        (item._1, item._2.size)  //size是查看有几个元素
      })

      //.foreach(println(_))
      .foreach(x => {
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
    val sql = "insert into AgriculturalMarketAnalysis4(province,marketCount) values(?,?)"

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
