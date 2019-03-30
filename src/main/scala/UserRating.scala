import org.apache.log4j._
import org.apache.spark._

object UserRating {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().
      setAppName("UserRatting").setMaster("local[*]")//.set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    //val businessFile = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/business.csv")
    val businessFile = sc.textFile(args(0))
    val businessLine = businessFile.map(parseBusinessFile)
    val business_CollegeAndUniversities = businessLine.filter(x => x._2.toUpperCase.contains("COLLEGES & UNIVERSITIES"))

   // val reviewFile = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/review.csv")
   val reviewFile = sc.textFile(args(1))
    val reviewLine = reviewFile.map(parseReviewFile)

    val user_ratings = business_CollegeAndUniversities.join(reviewLine).map(x => (x._2._2._1, x._2._2._2)).distinct();


    val formatted_output = user_ratings.map(line =>s"${line._1}\t${line._2}")


    //formatted_output.foreach(println)
    // formatted_output.coalesce(1).saveAsTextFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/target/output/UsersRating")

    formatted_output.coalesce(1).saveAsTextFile(args(2))

  }
  def parseBusinessFile(line: String) =
  {
    val fields = line.split("::")
    val business_id = fields(0)
    val categories = fields(2)
    (business_id, categories)
  }

  def parseReviewFile(line: String) =
  {
    val fields = line.split("::")
    val user_id = fields(1)
    val business_id = fields(2)
    val stars = fields(3).toDouble
    (business_id, (user_id, stars))
  }
}