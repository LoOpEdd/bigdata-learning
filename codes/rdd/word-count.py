from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("./data/book.txt")
words = input.flatMap(lambda x: x.split())

# countByValue approach
wordCounts = words.countByValue()

# map approach
wordCounts2 = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCounts2_map = wordCounts2.collect()

print("countByValue approach")
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(word, count)


print("map approach")
for word in wordCounts2_map:
    cleanWord = word[0].encode('ascii', 'ignore')
    if cleanWord:
        print(word[0], word[1])
