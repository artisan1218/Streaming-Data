import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.{File, PrintWriter}
import scala.collection.mutable._
import scala.util.Random

object task1 {

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val input_file_path_first = args(0)//"C:/Users/11921/OneDrive/FilesTransfer/DSCI 553/Assignments/Assignment6/business_first.json"
    val input_file_path_second = args(1)// "C:/Users/11921/OneDrive/FilesTransfer/DSCI 553/Assignments/Assignment6/business_second.json"
    val output_file_path = args(2)//"task3ScalaOutput.csv"

    val conf = new SparkConf().setAppName("task3").setMaster("local[*]")
        .set("spark.executor.memory", "4g")
        .set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)

    val city_int = sc.textFile(input_file_path_first)
                                      .map(line=>parseJson(line))
                                      .filter(city=>city.length!=0)
                                      .map(city=>BigInt(math.abs(city.hashCode)))
                                      .distinct()
                                      .collect().toList

    val num_func = 5
    val len_bit_array = 10000
    val ab_tuple = generateHashFunc(num_func)
    val bloom_filter = buildBloomFilter(city_int, len_bit_array, ab_tuple)

    val prediction = sc.textFile(input_file_path_second)
      .map(line=>parseJson(line))
      .map(city=>makePrediction(city, ab_tuple, bloom_filter, len_bit_array))
      .collect()

    outputResult(prediction, output_file_path)

  }

  def outputResult(prediction:Array[Int], path:String)={
    val file = new PrintWriter(new File(path))
    file.write(prediction.mkString(" "))
    file.close()
  }

  def makePrediction(city:String, ab_tuple:Tuple2[ListBuffer[BigInt], ListBuffer[BigInt]], bloom_filter:Set[BigInt], len:Int):Int={
    if(city.length == 0) {
      return 0
    }else{
      val cityInt = BigInt(math.abs(city.hashCode))
      val param_a_list = ab_tuple._1
      val param_b_list = ab_tuple._2
      val zipped = param_a_list zip param_b_list
      var contain = true
      for(zipped_param <- zipped){
        val a = zipped_param._1
        val b = zipped_param._2
        val hashed = calculateHashedValue(a, b, BigInt(len), cityInt)
        if(!bloom_filter.contains(hashed)) {
          contain = false
        }
      }
      if(contain==true){
        return 1
      }else{
        return 0
      }
    }
  }

  def buildBloomFilter(city_int:List[BigInt], len_bit_array:Int, ab_tuple:Tuple2[ListBuffer[BigInt], ListBuffer[BigInt]]): Set[BigInt]={
    var bloom_filter = Set[BigInt]()
    val param_a_list = ab_tuple._1
    val param_b_list = ab_tuple._2

    val zipped = param_a_list zip param_b_list
    for(city<-city_int){
      for(zipped_param <- zipped){
        val a = zipped_param._1
        val b = zipped_param._2
        val hashed = calculateHashedValue(a, b, BigInt(len_bit_array), city)
        bloom_filter += hashed
      }
    }
    return bloom_filter
  }

  def calculateHashedValue(a:BigInt, b:BigInt, m:BigInt, x:BigInt): BigInt={
    return (a * x + b) % m
  }

  def generateHashFunc(numFunc: Int): Tuple2[ListBuffer[BigInt], ListBuffer[BigInt]] = {
    val r = new Random()
    var a_list = ListBuffer[BigInt]()
    var b_list = ListBuffer[BigInt]()
    var i = 0
    while(i<numFunc){
      a_list += BigInt(r.nextInt(999999999))
      b_list += BigInt(r.nextInt(999999999))
      i+=1
    }
    return Tuple2(a_list, a_list)
  }

  def parseJson(line: String): String = {
    implicit val formats = DefaultFormats
    val jsonLine = parse(line)
    val city = (jsonLine \ "city").extract[String]
    return city
  }

}
