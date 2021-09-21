from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpent")
sc = SparkContext(conf=conf)

def get_orders(line):
    fields = line.split(',')
    id = int(fields[0])
    spent = float(fields[2])
    return (id, spent)

lines = sc.textFile("./data/customer-orders.csv")
orders = lines.map(get_orders)
total_spent = orders.reduceByKey(lambda x, y: round(x + y, 2))
total_spent_ordered = total_spent.map(lambda x: (x[1], x[0])).sortByKey()
results = total_spent_ordered.collect()

for result in results:
    print(result[1], result[0])