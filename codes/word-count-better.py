from pyspark import SparkConf, SparkContext
import re

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCountBetter")
sc = SparkContext(conf=conf)

input = sc.textFile("./data/book.txt")
words = input.flatMap(normalizeWords)

# map approach
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
result = wordCountsSorted.collect()

print("map approach")
for word in result:
    cleanWord = word[1].encode('ascii', 'ignore')
    if cleanWord:
        print(word[1], word[0])
