package com.sunliang.sitech

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sun on 2019/1/17.
  */
object OrderPre {
  def main(args:Array[String]): Unit = {
  }

  def calProductCounts(): Unit = {
    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rawdata = sc.textFile("/data/rm_bsscbssorder_all_info.csv")
    val records = rawdata.map(line => {
      val pcode = line.replace("\"", "").split(",")(5)
      (pcode, 1)
    }).reduceByKey(_ + _).map(k => k._1 + " " + k._2)
    records.saveAsTextFile("/data/pcounts")
  }

  def distinct(): Unit = {
    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rawdata = sc.textFile("/data/sunliang/prodinfo.csv")
    val records = rawdata.map(line => {
      val pcode = line.replace("\"", "").split(",")(0)
      pcode
    }).distinct().saveAsTextFile("/data/sunliang/uniproduct")
  }

  def joinOrder(): Unit = {
    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val orderdata1 = sc.textFile("/data/rm_bsscbssorder_all_info.csv").count()

    val proddata = sc.textFile("/data/sunliang/uniproduct").map{ line =>
      var map = Map[String, String]()
      map += (line -> "1")
      map
    }.collect().flatten.toMap

    sc.textFile("/data/rm_bsscbssorder_all_info.csv").map{ line =>
      val d = line.replace("\"", "").split(",")
      Array(d(3), d(5), d(8))
    }.filter(n => proddata.contains(n(1))).map(_.mkString(",")).count()//.saveAsTextFile("/data/sunliang/order_records_filter")

    //过滤掉同一产品订购多次的
    sc.textFile("/data/sunliang/order_records_filter").map(line =>{
      val f = line.split(",")
      (f(0) + "@" + f(1),f(2))
    }).groupByKey().map(k=>{
      val max_date = k._2.max
      val str = k._1.split("@")
      str(0) + "," + str(1) + "," + max_date
    }).saveAsTextFile("/data/sunliang/ph_pid")

    //统计用户订购纪录大于等于5次的
    sc.textFile("/data/sunliang/ph_pid").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 4).count()

    sc.textFile("/data/sunliang/ph_pid").map(line =>{
      val f = line.split(",")
      (f(0), f(1) + "@" + f(2))
    }).groupByKey().filter(n => n._2.size > 4).map( k=> {
      k._1 + " " + k._2.size
    }).foreach(println)

    //剔掉同一用户小于5次的订购纪录
 /*   sc.textFile("/data/sunliang/ph_pid").map(line =>{
      val f = line.split(",")
      (f(0), f(1) + "@" + f(2))
    }).groupByKey().filter(n => n._2.size > 4).map(k => {
      (k._1,k._2.toList)
    }).map(k =>{
      import scala.collection.mutable.ListBuffer
      var buffer = new ListBuffer
      for(v1 <- k._2){
        buffer += (1,1)
      }
    })*/


    sc.textFile("/data/sunliang/ph_pid").map(line =>{
      val f = line.split(",")
      (f(0), f.slice(1,3))
    }).groupByKey().map(k=>{
      val pid = k._2.filter(n => n.size > 4).map(n=> (k._1,n(0),n(1)))
      pid
    }).foreach(println)

    sc.textFile("/data/sunliang/ph_pid").map(line =>{
      val f = line.split(",")
      (f(0), f.slice(1,3))
    }).groupByKey().map(k=>{
      val max_date = k._2.map(_(1).toInt).max.toString
      val pid = k._2.filter(n => max_date.equals(n(1))).last.apply(0)
      k._1 + "," + pid
    }).saveAsTextFile("/data/ph_pid_train")
  }

  def ncf(): Unit = {
    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    sc.textFile("/data/sunliang/ncf_train_new2").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 3).count()

    sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 3).map(f => (f._1,f._2)).foreach(println)

    sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 3).map(_._1).saveAsTextFile("/data/sunliang/ncf_phone")


    val p = sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 3).map( f => {
      f._1
    }).reduce((f1,f2) => {
      f1 + "," + f2
    })

    sc.textFile("/data/ncf_train").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      p.indexOf(f._1) > 0
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/ncf_train_new")


    sc.textFile("/data/ncf_test").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      p.indexOf(f._1) > 0
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/ncf_test_new")





/*    val ph = sc.textFile("/data/order_ph_pid_dt").count()

    val ph = sc.textFile("/data/order_ph_pid_dt").map(line =>{
      val d = line.split(",")
      (d(0), d(1))
    }).groupByKey().filter(f => f._2.toArray.distinct.size>4).foreach(println)*/

    sc.textFile("/data/order_ph_pid_dt").map(line =>{
      val d = line.split(",")
      d(0)
    }).distinct().count()

    val ph = sc.textFile("/data/order_ph_pid_dt").map(line =>{
      val d = line.split(",")
      (d(0), d(1))
    }).groupByKey().filter(f => f._2.toArray.distinct.size>4).map(f=>(f._1, 1))

    val ph = sc.textFile("/data/order_ph_pid_dt").map(line =>{
      val d = line.split(",")
      (d(0), d(1))
    }).groupByKey().filter(f => f._2.toArray.distinct.size < 4)

    sc.textFile("/data/order_ph_pid_dt").map(line => {
      line.split(",")}).filter(_.length > 2).map(d=>{
      (d(0), (d(1), d(2)))
    }).join(ph).map(f => {
      f._1
    }).distinct().count()

    val ord_dataset = sc.textFile("/data/order_ph_pid_dt").map(line => {
      line.split(",")}).filter(_.length > 2).map(d=>{
      (d(0), (d(1), d(2)))
    }).join(ph).foreach(println)

/*    val ord_dataset = sc.textFile("/data/order_ph_pid_dt").map(line => {
      line.split(",")}).filter(_.length > 2).map(d=>{
      (d(0), (d(1), d(2)))
    }).join(ph).map(f=>(f._1, f._2._1)).foreach(println)*/

    sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f.slice(0,1), f.slice(1,f.length))
    }).sortBy(_._1(0)).map(f => {
      val v = Array.concat(f._1,f._2).mkString(",")
      v
    }).saveAsTextFile("/data/ncf_train1")

    sc.textFile("/data/ncf_test").map(line=>{
      val f = line.split(",")
      (f.slice(0,1), f.slice(1,f.length))
    }).sortBy(_._1(0)).map(f => {
      val v = Array.concat(f._1,f._2).mkString(",")
      v
    }).saveAsTextFile("/data/ncf_test1")

    sc.textFile("/data/ncf_all1").map(line=>{
      val f = line.split(",")
      (f.slice(0,1), f.slice(1,f.length))
    }).sortBy(_._1(0)).map(f => {
      val v = Array.concat(f._1,f._2).mkString(",")
      v
    }).saveAsTextFile("/data/ncf_all")

    sc.textFile("/data/rm_bsscbssorder_all_info.csv").map(line => {
      val d = line.replace("\"", "").split(",")
      d
    }).filter(f => {
      f(5).equals("")
    }).count()

    sc.textFile("/data/rm_bsscbssorder_all_info.csv").map(line => {
      line.replace("\"", "").split(",")
    }).filter{f => f(5).equals("")}.map{d => Array(d(3), d(5), d(8)).mkString(",")}.saveAsTextFile("/data/sunliang/order_ph_pid_dt")

    import scala.collection.breakOut
    val selector = Seq(228,18,204,19,41,125,43,36,35,155,201,23,44,257,157,198,21,200,242,202,147,246,114,34,208,50,14,38,16,20,211,117,17,15,25).map(_-1) // with 1 started
    val user_rec = sc.textFile("/data/userlabel20181211_rm_tr").map( line =>{
      val d = line.split(",")
      val k = d(50) //device_number
      val v = Array.concat(d.slice(1, 50) , d.slice(51, d.size)).mkString(",")
      val newv = selector.map(v)(breakOut).toArray.mkString(",")
      (k, newv)
    }).saveAsTextFile("/data/sunliang/user_rec")


  }

  def createdata(): Unit = {

    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //训练集纪录数 2459596
    sc.textFile("/data/ncf_train").count()

    //训练集号码个数 920816
    sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct().count()

    //训练集大于等于4条保留的号码个数 188189
    sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 3).count()

    //训练集小于4条需要剔除的号码个数 732627
    sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 < 4).count()

    //新训练集条数 1163523
    sc.textFile("/data/sunliang/ncf_train_new").count()

    //新训练集的号码个数 188188
    sc.textFile("/data/sunliang/ncf_train_new").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct().count()

    //新训练集中订购纪录小于4条的号码数 0
    sc.textFile("/data/sunliang/ncf_train_new").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 < 4).count()

    val p = sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 3).map( f => {
      f._1
    }).reduce((f1,f2) => {
      f1 + "," + f2
    })

    sc.textFile("/data/ncf_train").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      p.indexOf(f._1) > 0
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/ncf_train_new")



    //测试集纪录数 222318
    sc.textFile("/data/ncf_test").count()

    //测试集号码数 222318
    sc.textFile("/data/ncf_test").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct().count()

    //新测试集纪录数 60160
    sc.textFile("/data/sunliang/ncf_test_new").count()

    //新测试集号码数 60160
    sc.textFile("/data/sunliang/ncf_test_new").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct().count()

    val p1 = sc.textFile("/data/sunliang/ncf_train_new").map(line=>{
      val f = line.split(",")
      f(0)
    }).reduce((f1,f2) => f1 + "," + f2)

    //新测试集
    sc.textFile("/data/ncf_test").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      p1.indexOf(f._1) > 0
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/ncf_test_new1")




  }

  def valid(): Unit = {
    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //920816
    val train_phone = sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct()

    //222318
    val test_phone = sc.textFile("/data/ncf_test").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct()

    //213307
    val phone = train_phone.intersection(test_phone).distinct().reduce{(f1,f2) => f1 + "," + f2}

    //保留在训练集里大于2条的号码
    val p = sc.textFile("/data/ncf_train").map(line=>{
      val f = line.split(",")
      (f(0), 1 )
    }).reduceByKey( _ + _ ).filter( _._2 > 1).map( f => {
      f._1
    }).reduce((f1,f2) => {
      f1 + "," + f2
    })

    sc.textFile("/data/ncf_train").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      p.indexOf(f._1) > 0 && phone.indexOf(f._1) > 0 //&& f._1 != "17699100928"
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/20190124/ncf_train_20190124_1")

    sc.textFile("/data/ncf_train").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      phone.indexOf(f._1) > 0
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/20190124/ncf_train_20190124_2")

    val p1 = sc.textFile("/data/sunliang/20190124/ncf_train_20190124_1").map(line=>{
      val f = line.split(",")
      f(0)
    }).reduce((f1,f2) => f1 + "," + f2)

    sc.textFile("/data/ncf_test").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      p1.indexOf(f._1) > 0 && phone.indexOf(f._1) > 0 //&& f._1 != "17699100928"
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/20190124/ncf_test_20190124_1")

    sc.textFile("/data/ncf_test").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      phone.indexOf(f._1) > 0
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/20190124/ncf_test_20190124_2")



    //新训练集条数 703336
    sc.textFile("/data/sunliang/20190124/ncf_train_20190124_2").count()

    //新训练集的号码个数 213306
    sc.textFile("/data/sunliang/20190124/ncf_train_20190124_2").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct().count()

    //新测试集条数 213306
    sc.textFile("/data/sunliang/20190124/ncf_test_20190124_2").count()

    //新测试集号码数 213306
    sc.textFile("/data/sunliang/20190124/ncf_test_20190124_2").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct().count()

    val train_phone1 = sc.textFile("/data/sunliang/20190124/ncf_train_20190124_1").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct()


    val test_phone1 = sc.textFile("/data/sunliang/20190124/ncf_test_20190124_1").map(line=>{
      val f = line.split(",")
      f(0)
    }).distinct()

    //17699100139
    val phone1 = train_phone1.subtract(test_phone1).foreach(println)

    //过滤掉多的一个
    sc.textFile("/data/sunliang/20190124/ncf_train_20190124_1").map(line => {
      val d = line.split(",")
      (d(0),d)
    }).filter(f => {
      f._1 != "17699100139"
    }).map( f => f._2.mkString(",")).saveAsTextFile("/data/sunliang/20190124/ncf_train_20190124_11")

    sc.textFile("/data/sunliang/20190124/ncf_test_20190124_2").map(line=>{
      val f = line.split(",")
      (f.slice(0,1), f.slice(1,f.length))
    }).sortBy(_._1(0)).map(f => {
      val v = Array.concat(f._1,f._2).mkString(",")
      v
    }).saveAsTextFile("/data/sunliang/20190124/ncf_test_20190124_22")

  }

  def nigative(): Unit = {
    val conf = new SparkConf().setAppName("Order records preprocess").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    sc.textFile("/data/sunliang/20190124/ncf_test_temp_1").map(line=>{
      val f = line.split(",")
      (f.slice(0,1), f.slice(1,f.length))
    }).sortBy(_._1(0)).map(f => {
      val v = Array.concat(f._1,f._2).mkString(",")
      v
    }).saveAsTextFile("/data/sunliang/20190124/ncf_test_temp_11")


    val m = sc.textFile("/data/plist_vec").map(line=>{
      val f = line.split(",")
      var str = ""
      val l = f.slice(1,f.length)
      for(item <- l) {
        str += "," + item
      }
      val map = Map(f(0) -> str)
      map
    }).reduce((f1,f2) => {
      f1 ++ f2
    })


    sc.textFile("/data/sunliang/20190124/ncf_test_temp_11").map(line=>{
      val f = line.split(",")
      val l = f.slice(1,f.length)
      var str = f(0)

      for(item <- l) {
        str  += m(item)
      }
      str
    }).saveAsTextFile("/data/sunliang/20190124/negative11")

  }
}
