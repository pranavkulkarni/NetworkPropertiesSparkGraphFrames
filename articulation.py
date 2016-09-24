# $SPARK_HOME/bin/spark-submit --driver-memory 4g --jars /home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar articulation.py 9_11_edgelist.txt
import sys
import pyspark
from pyspark.sql.functions import *

sc = pyspark.SparkContext("local[4]", "Graph Data Mining", pyFiles=['/home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar'])
from graphframes import *

def createGraphFrame(filename):
    graphRDD = sc.textFile(filename)
    delimiter = ","
    graphRDD = graphRDD.map(lambda row: row.split(delimiter))
    sqlContext = pyspark.sql.SQLContext(sc)
    e = sqlContext.createDataFrame(data = graphRDD, schema = ["src", "dst"])
    v = e.select(e.src).distinct().select(col("src").alias("id"))
    g = GraphFrame(v, e)
    return g

def articulation(g):
    art = []
    count = 1
    #print g.vertices.count()
    #vertices = g.vertices.collect()
    #result = g.connectedComponents()
    oriNumOfComponents = g.connectedComponents().agg(countDistinct("component").alias("num")).collect()[0].num
    print "ori", oriNumOfComponents
    for row in g.vertices.collect():
        print count
        #e = g.edges
        #e = e.filter("src != '"+row.id+"'").filter("dst != '"+row.id+"'")
        numOfComponents = GraphFrame(g.vertices.filter("id != '"+row.id+"'"),g.edges).connectedComponents().agg(countDistinct("component").alias("num")).collect()[0].num
        if numOfComponents > oriNumOfComponents:
            art.append((row.id,1))
        else:
            art.append((row.id,0))
        count = count + 1
    sqlContext = pyspark.sql.SQLContext(sc)
    artDF = sqlContext.createDataFrame(art,["id", "articulation"])
    return artDF
    

def main():
    filename = sys.argv[1]
    g = createGraphFrame(filename)
    art = articulation(g)
    art.show()
   

if __name__ == '__main__':
   main()
