from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("PopularMovies").getOrCreate().sparkContext


def parsedLines(line):
    fields = line.split()
    movieID = fields[1]
    return movieID


lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
