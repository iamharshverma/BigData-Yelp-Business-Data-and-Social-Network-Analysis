import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object TopTenBusinessLocatedInGivenCity {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    // val sc = new SparkContext("local[*]", "MinTemperatures")
    val conf = new SparkConf().
      setAppName("Top10BusinessInNY").setMaster("local[*]")//.set("spark.driver.bindAddress","127.0.0.1")
    val sc = new SparkContext(conf)
   // val businessFile = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/business.csv")
    val businessFile = sc.textFile(args(0))
    val businessSchema = StructType(Array(
      StructField("business_id", StringType, true),
      StructField("full_address", StringType, true),
      StructField("categories", StringType, true)))

    val reviewSchema = StructType(Array(
      StructField("review_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("business_id", StringType, true),
      StructField("stars", StringType, true)))


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rowbusinessRDD = businessFile.map(line => Row.fromSeq(line.split("::")))
    val businessDF = sqlContext.createDataFrame(rowbusinessRDD, businessSchema)
    businessDF.createOrReplaceTempView("business_view")

    val rawReviewData = sc.textFile(args(1))
    //val rawReviewData = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/review.csv")
    val rowReviewRDD = rawReviewData.map(line => Row.fromSeq(line.split("::")))
    val reviewDF = sqlContext.createDataFrame(rowReviewRDD, reviewSchema)
    reviewDF.createOrReplaceTempView("review_view")

    val id_nyc_view = sqlContext.sql("""SELECT business_id, full_address, categories FROM business_view where UPPER(full_address) LIKE '%NY%'""")
    id_nyc_view.createOrReplaceTempView("id_nyc_view")
    val joined_DF = sqlContext.sql("""SELECT business_id , AVG(stars) as average FROM review_view GROUP BY business_id ORDER BY average DESC""")
    val joined_DF_Final = id_nyc_view.join(joined_DF, ("business_id")).distinct().sort( "average").orderBy(desc("average")).limit(10)
    joined_DF_Final.show()
    //joined_DF_Final.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("sep", "\\t").save("/Users/harshverma/Documents/Project_repo/ScalaBigData/target/output/TopBusiness")
    joined_DF_Final.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("sep", "\\t").save(args(2))

  }

}