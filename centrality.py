# $SPARK_HOME/bin/spark-submit --jars /home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar centrality.py
import sys
import pyspark
from pyspark.sql.functions import *

sc = pyspark.SparkContext("local", "Graph Data Mining", pyFiles=['/home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar'])
from graphframes import *

def createGraphFrame():
    sqlContext = pyspark.sql.SQLContext(sc)
    v = sqlContext.createDataFrame([ ("A","A"), ("B","B"), ("C","C"), ("D","D"), ("E","E"), ("F","F"), ("G","G"), ("H","H"), ("I","I"), ("J","J"),], ["id","name"])
    e = sqlContext.createDataFrame([ ("A","B"), ("A","C"), ("A","D"), ("B","A"), ("B","C"), ("B","D"), ("B","E"), ("C","A"), ("C","B"), ("C","D"), ("C","F"), ("C","H"), ("D","A"), ("D","B"), ("D","C"),
    ("D","E"), ("D","F"), ("D","G"), ("E","B"), ("E","D"), ("E","F"), ("E","G"), ("F","C"), ("F","D"), ("F","E"), ("F","G"), ("F","H"), ("G","D"), ("G","E"), ("G","F"), ("H","C"), ("H","F"), ("H","I"),
    ("I","H"), ("I","J"), ("J","I"),], ["src","dst"])
    g = GraphFrame(v,e)
    return g

def findCentrality(g):
    #print g.vertices
    lm = map(lambda row: row.id, g.vertices.collect())
    print lm
    results = g.shortestPaths(landmarks = lm)
    results.select("id", "distances").show()
    shortPathLens = results.select("id", explode("distances"))
    shortPathLens.show()
    centarlityMatrix = shortPathLens.groupBy(shortPathLens.id).agg({"value": "sum"})
    centarlityMatrix = centarlityMatrix.select(col("id"),col("sum(value)").alias("closeness"))
    centarlityMatrix =  centarlityMatrix.map(lambda row: (row.id,1/float(row.closeness))).toDF().select(col("_1").alias("id"), col("_2").alias("closeness"))
    return centarlityMatrix
    

def main():
    g = createGraphFrame()
    centrality = findCentrality(g)
    rankedCentrality  = centrality.sort(centrality.closeness.desc())
    rankedCentrality.show()
   

if __name__ == '__main__':
   main()
