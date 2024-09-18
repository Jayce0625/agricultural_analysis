package lxj.agricultural

import lxj.agricultural.AgriculturalMarketAnalysis2.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * 根据农产品种类数量，统计每个省份排名前3名的农产品市场
 */
object AgriculturalMarketAnalysis3 {
  //创建SparkConf对象，设置application与master地址
  //创建SparkConf对象，是所有spark程序的执行入口
  private val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("dataframe"))
  //从hdfs上面读取文件内的数据，存入RDD(弹性分布式数据集)
  private val productRDD: RDD[String] = sc.textFile("hdfs://192.168.159.129:9000/datas/product.txt")

  def main(args: Array[String]): Unit = {
    //将读取到的数据进行筛选---》首先使用空白部分隔开，再就是得到一行与6个部分的数据
    productRDD.filter(_.split("\\s+").length == 6)
      //使用map对每个数据使用空白分开，选出一行之中的第4，5，1个数据，构造为元组的形式
      .map(line => {
        val product = line.split("\\s+")
        (product(3), product(4)) -> product(0)
      })
      //使用distinct去除元组中重复的数据
      .distinct() //  ((北京顺义石门蔬菜批发市场,北京),生姜)
      .map(item => item._1 -> 1) //((北京顺义石门蔬菜批发市场,北京),1)---》将元组中的key部分都设置为1
      .reduceByKey(_ + _) // 计算出相同key的数量
      //就是将元组中的第一个元素中的第二个（就是value）来进行分组
      .groupBy(_._1._2) // (青海,CompactBuffer(((西宁仁杰粮油批发市场有限公司,青海),3)))
      //扁平化处理，先处理在压扁
      .flatMap(item => {
        val province = item._1
        val arr = item._2.toBuffer   //用于返回集合中所有元素的缓冲区
        //按照((西宁仁杰粮油批发市场有限公司,青海),3)中的3位置进行排序的倒序（从小到大，转变为从大到小）
        arr.sortBy(_._2).reverse.take(3)   //表示取出前3个元素
          .map(x => (
            province, x._1._1, x._2 // (辽宁,辽宁阜新市瑞轩蔬菜农副产品综合批发市场,53)      x._1._1表示x元组中第1个（key）位置中的第1个（key）  x._2表示x元组中的第二个（value）位置
          ))
      })
      //.foreach(println(_))  //进行结果数据的输出
      .foreach(x => {
        val arrs = Array(x._1,x._2,x._3.toString)
       // saveResult(arrs)
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
    val sql = "insert into AgriculturalMarketAnalysis3(province,market,speciesNumbers) values(?,?,?)"

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
