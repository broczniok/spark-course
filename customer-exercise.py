from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerExercise")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    cost = fields[2]
    return (int(customerID), float(cost))

lines = sc.textFile("customer-orders.csv")
parsedLines = lines.map(parseLine)
customerCost = parsedLines.reduceByKey(lambda x, y: x+y)
customerCostSorted = customerCost.map(lambda x: (x[1], x[0])).sortByKey()

results = customerCostSorted.collect()


for result in results:
    print(result[1],':',result[0])