import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

brokers, topic = sys.argv[1:]

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "WordCount")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to Kafka
lines = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

# Split each line into words
words = lines.flatMap(lambda line: line[1].split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

