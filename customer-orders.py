from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("customer-orders")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    return (int (fields[0]), float(fields[2]))

input = sc.textFile("file:///SparkCourse/customer-orders.csv")

mappedInput = input.map(parseLine)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x+y)

#Changed for Python 3 Compatibility
# flipped = totalByCustomer.map(lambda(x,y):(y,x))

flipped = totalByCustomer.map(lambda x:(x[1], x[0]))
totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect();

for result in results:
    print(result)