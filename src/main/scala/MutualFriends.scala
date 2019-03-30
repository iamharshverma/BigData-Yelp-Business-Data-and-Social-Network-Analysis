import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._


object HelloScala {
  def main(args: Array[String]): Unit = {
    println("Ruuning Program to find Mutual Friends list")
  }
}

object MutualFriends {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().
      setAppName("MutualFriends").setMaster("local[*]")//.set("spark.driver.bindAddress","127.0.0.1")
    val sc = new SparkContext(conf)

    // For Machine Run with arguments
    //    if (args.length < 4) {
    //      println("Enter proper number of args")
    //      System.exit(1)
    //    }
    val file = sc.textFile(args(0))

// For local Run
   // var file = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/soc-LiveJournal1Adj.txt")

    val parsedFile = file.map(x => x.split("\t")).filter(x => x.length == 2).map(x => (x(0), x(1).split(",")))

    val mapped_output = parsedFile.map(x=>
    {
      //var map1 = scala.collection.mutable.Map[String, List[String]]()
      val friend1 = x._1
      val friends = x._2.toList
      for(friend2<- friends) yield
        {
          if(friend1.toInt< friend2.toInt)
            (friend1+","+friend2 -> friends)
          else
            (friend2+","+friend1 -> friends)
        }

    }
    )

    val flatten_output =   mapped_output.flatMap(identity).map(x=> (x._1,x._2)).distinct.reduceByKey((x,y)=> (x.intersect(y)))
    val friend_count = flatten_output.map(x=> x._1 + "\t"+ x._2.length)
    //friend_count.coalesce(1).saveAsTextFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/target/output/MutualFriendsSpark")
    friend_count.coalesce(1).saveAsTextFile(args(1))

  }
}
