from pyspark.sql import SparkSession
import collections


sc = SparkSession.builder.appName("RatingsHistogram").getOrCreate().sparkContext

lines = sc.textFile("ml-100k/u.item")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
