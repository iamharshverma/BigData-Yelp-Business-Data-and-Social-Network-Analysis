import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._

  object TopTenMutualFriendDetail {
    def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("TopTenMutualFriendDetail"). setMaster("local[*]")//.set("spark.driver.bindAddress","127.0.0.1")
      val sc = new SparkContext(conf)


     // val file = sc.textFile(args(0))
      var file = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/soc-LiveJournal1Adj.txt")

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
      val friend_count = flatten_output.map(x=> (x._1, x._2.length)).map(_.swap).sortByKey(false).take(10).map(x=> (x._1,(x._2.split(",")(0),x._2.split(",")(1))))

      friend_count.foreach(println)
      val userdata_file = sc.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/userdata.txt")
      //val userdata_file = sc.textFile(args(1))
      val read_userdata = userdata_file.map(parseUserData)

      val friend_countRDD = sc.parallelize(friend_count)

      val RDD1 =friend_countRDD.map(x=> (x._2._1,(x._2._2,x._1))).join(read_userdata).map(x=> (x._2._1._1, (x._1,x._2._1._2,x._2._2)))
      val RDD2 = RDD1.join(read_userdata).map(x=>(x._2._1._2 ,(x._2._1._3,x._2._2)))

      RDD2.foreach(x => println(x._1+"\t"+x._2._1._1+"\t"+x._2._1._2+"\t"+x._2._1._3+"\t"+x._2._2._1+"\t"+x._2._2._2+"\t"+x._2._2._3))
      val formatted_output = RDD2.map(x=>(x._1+"\t"+x._2._1._1+"\t"+x._2._1._2+"\t"+x._2._1._3+"\t"+x._2._2._1+"\t"+x._2._2._2+"\t"+x._2._2._3) )


      formatted_output.coalesce(1).saveAsTextFile(args(2))
      //formatted_output.coalesce(1).saveAsTextFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/target/output/TopFriendsDetails")


      // For information
      // friend_countRDD.foreach(println)
      // RDD2.foreach(println)
      // RDD2.map(_.productIterator.mkString("\t")).foreach(println)
      // val joinResult = read_userdata.map { case (k, v) => (k, v, smaller.value.get(k)) }

      // for (x<-RDD2)
      // {
      //   val count = x._1
      //   val firstName1 = x._2._1._1
      //   val  lastName1= x._2._1._2
      //   val address1 = x._2._1._3
      //   val firstName2 = x._2._2._1
      //   val  lastName2= x._2._2._2
      //   val address2 = x._2._2._3
      //   println(s"$count\t$firstName1\t$lastName1\t$address1\t$firstName2\t$lastName2\t$address2")
      // }
      //
    }


    def parseUserData(line:String) =
    {
      val fields = line.split(",")
      val userID = fields(0)
      val  firstname = fields(1)
      val lastname = fields(2)
      val address =fields(3)
      (userID,(firstname,lastname,address))
    }
}
