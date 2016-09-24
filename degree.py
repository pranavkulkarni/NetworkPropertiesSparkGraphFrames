# $SPARK_HOME/bin/spark-submit --jars /home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar degree.py stanford_graphs/amazon.graph.small
import sys
import pyspark
from pyspark.sql.functions import *
import math
import matplotlib.pyplot as plt
import networkx as nx

sc = pyspark.SparkContext("local", "Graph Data Mining", pyFiles=['/home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar'])
from graphframes import *

def createGraphFrame(filename, delimiter):
    graphRDD = sc.textFile(filename)
    if delimiter == " ":
        graphRDD = graphRDD.zipWithIndex().filter(lambda (row,index): index > 0).keys()
    srcToDstRDD = graphRDD.map(lambda row: row.split(delimiter))
    dstToSrcRDD = graphRDD.map(lambda row: (row.split(delimiter)[1], row.split(delimiter)[0]))
    graphRDD = srcToDstRDD.union(dstToSrcRDD)
    sqlContext = pyspark.sql.SQLContext(sc)
    e = sqlContext.createDataFrame(data = graphRDD, schema = ["src", "dst"])
    v = e.select(e.src).distinct().select(col("src").alias("id"))
    g = GraphFrame(v, e)
    return g

def degDist(g):
    outDeg = g.outDegrees # outDegrees and inDegrees are same now for undirected graph as edges were added twice
    degDist = outDeg.groupBy("outDegree").count()
    degDist = degDist.select(col("outDegree").alias("degree"), col("count").alias("freq"))
    return degDist

def networkxGraphs():
    g = nx.gnm_random_graph(n = 200, m = 800)
    deg = g.degree()
    degDist = dict()
    for k, v in deg.iteritems():
        if degDist.get(v) is not None:
            degDist[v] = degDist[v] + 1
        else:
            degDist[v] = 1
    sqlContext = pyspark.sql.SQLContext(sc)
    print degDist
    degDistDF = sqlContext.createDataFrame(data = degDist.items(), schema = ["degree", "freq"])
    degDistDF.show()
    checkPowerLaw(degDistDF, "networkx")

def checkPowerLaw(degDistDF, filename):
    largeK = degDistDF.sort(col("degree").desc())
    n = degDistDF.select(sum(degDistDF['freq']).alias("total")).collect()[0].total
    print largeK.count()
    numOfK = int(largeK.count()*0.50)
    print numOfK
    largeK = largeK.head(numOfK)
    #print type(largeK)
    #print largeK
    x = map((lambda row: (row.degree, -math.log(float(row.freq)/n)/math.log(float(row.degree)))), largeK)
    #print "\nFor graph: ", filename
    print x
    xs = [c[0] for c in x]
    ys = [c[1] for c in x]
    plt.plot(xs, ys, 'ro')
    plt.title(filename)
    plt.show()


def main():
    filename = sys.argv[1]
    delimiter = ","
    if "large" in filename:
        delimiter = " "
    if "networkx" in filename:
        networkxGraphs()
        sys.exit()
    g = createGraphFrame(filename, delimiter)
    degDistDF = degDist(g)
    degDistDF.show()
    checkPowerLaw(degDistDF, filename)

if __name__ == '__main__':
    main()
