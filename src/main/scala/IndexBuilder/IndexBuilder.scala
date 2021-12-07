package IndexBuilder
// Each library has its significance, I have commented when it's used
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object IndexBuilder {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("question4")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = SparkSession.builder().appName("Question4").config("spark.master", "local").getOrCreate()

  def main (args:Array[String]): Unit = {
    val inputPath = args(0)
    println(inputPath)
    val fileString = sc.textFile(inputPath)
    val fileHeader = "ID,Name,Age,Gender,CountryCode,Salary"

    val schema = StructType(fileHeader.split(",").map(fieldName => StructField(fieldName,StringType, true)))
    val rowRDD = fileString.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5)))
    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //dataFrame.createOrReplaceTempView("Customers")
    var newDataframe = dataFrame.repartition(10)
    println(newDataframe.rdd.partitions.size)
    var originalRDD = newDataframe.rdd
    var newRDDIndexWithID = newDataframe.rdd.mapPartitionsWithIndex((index, iterator)=>{
      iterator.map { x => (index, x.get(0)) }
    })
    //newRDDIndexWithID.foreach(println)
    // compare through first index, i.e id
    var comparedField = 0
    var conditionVal = "18"

    println("this is the first result")

    //query without index
    val t1 = System.nanoTime

    var resultRDD = originalRDD.filter(x => x(0)==conditionVal)
    resultRDD.foreach(println)

    val duration1 = (System.nanoTime - t1) / 1e9d
    println("this is the runtime without index")
    println(duration1)

    var test = (1,2)

    //query with index
    val t2 = System.nanoTime
    //first get index
    var index = newRDDIndexWithID.filter(x=>x._2==conditionVal).map(x=>x._1)
    var collectIndexRes = index.collect()

    //query with index


    var resultRDDWithIndex = originalRDD.mapPartitionsWithIndex((index, iterator)=>{
      if (collectIndexRes.contains(index)) {
      iterator.filter(x=>{x.get(0) == conditionVal})
      }else
        {
          iterator.filter(x=>{false})
        }

    })
    println("this is the second result")
    resultRDDWithIndex.foreach(println)
    val duration2 = (System.nanoTime - t2) / 1e9d
    println("this is the runtime with index")
    println(duration2)
    sc.stop()
  }
}

