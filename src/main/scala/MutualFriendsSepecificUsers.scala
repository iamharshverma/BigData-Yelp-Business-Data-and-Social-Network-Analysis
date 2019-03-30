import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


  object MutualFriendsSpecificUsers {

    def main(args : Array[String]){

      val config = new SparkConf()
        .setAppName("Mutual Friends")
        .setMaster("local[*]")

      val sparkContext = new SparkContext(config)

      val userA = readLine("Enter id for User A : ")

      val userB = readLine("Enter id for User B : ")

      var inputFIle = sparkContext.textFile("/Users/harshverma/Documents/Project_repo/ScalaBigData/src/resources/soc-LiveJournal1Adj.txt")

      //val inputFIle = sparkContext.textFile("hdfs://localhost:9000/input")
      //0, 28	53,24,17,83,89,85,38

      val userAfriends = inputFIle.map(x=>x.split("\\t"))
        .filter(x => (x.size == 2))
        .filter(x=>userB==x(0))
        .flatMap(x=>x(1).split(","))

      val userBfriends = inputFIle.map(x=>x.split("\\t"))
        .filter(x => (x.size == 2))
        .filter(x=>userA==x(0))
        .flatMap(x=>x(1).split(","))

      val mutualFriendsList = userBfriends.intersection(userAfriends).collect()

      val answer=userA+", "+userB+"\t"+mutualFriendsList.mkString(",")
      val answerWithLength= userA+", "+userB+"\t"+mutualFriendsList.length
      println(answerWithLength);
      //answerWithLength.saveAsTextFile("hdfs://localhost:9000/output")
    }

}
