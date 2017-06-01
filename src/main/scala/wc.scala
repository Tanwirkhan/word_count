/**
  * Created by Tanwir on 01-06-2017.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wc {

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().
      setMaster("local").
      setAppName("word count")
    val sc = new SparkContext(conf)
    val randomtext = sc.textFile(args(0))
    randomtext.flatMap(rec=> rec.split(" ")).
      map(rec =>(rec, 1)).
       reduceByKey((agg, value) => agg + value).
       saveAsTextFile(args(1))

  }


}
