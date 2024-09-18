package lxj.agricultural

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 统计每个省份的农产品市场总数
 */
object AgriculturalMarketAnalysis1 {
  // 构建sparkConf对象,设置application名称和master地址
  // 构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
  //它内部会构建 DAGScheduler和 TaskScheduler 对象
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("dataframe"))

  //从hadoop的hdfs里面读取文件数据
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    //按照一个或者多个tab键切割(\s匹配任何空白字符，包括空格、制表符、换页符等等)
    //进行过滤--》符合使用tab键隔开的，有6个字段的数据筛选出来
    val provinceRDD: RDD[(String, String)] = productRDD.filter(_.split("\\s+").length == 6)
      .map(line => {
        val parts: Array[String] = line.split("\\s+")   //使用map将每行中的数据使用空白切割开，放入数组
        (parts(4), parts(3))      //取数组中第5、第4个数据
      })
    //去除重复数据，按照key分组计算
    val result: collection.Map[String, Long] = provinceRDD.distinct()  //使用distinct进行去重
      .countByKey()              //countByKey作用于value不可以进行数值计算的RDD，不关心value的内部情况，只计算value的个数
    result.foreach(println(_))
    result.foreach(x => {
      println(x._1 + "省" + "市场数量有" + x._2 + "个")

      //保存数据到数据库
      val arrs = Array(x._1,x._2.toString) //将每个元组转换为string类型的数组
      //saveResult(arrs)  //调用连接数据库并存入数据库的方法，将转换后的数组存入mysql数据库
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
    val sql = "insert into AgriculturalMarketAnalysis1(province,count) values(?,?)"

    // 3、获取PreParedStatement
    try {
      val ps: PreparedStatement = connection.prepareStatement(sql)

      // 4、获取数据，给？赋值------>就是将数组对应的数据赋值到相对应的数据库中的位置上
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
