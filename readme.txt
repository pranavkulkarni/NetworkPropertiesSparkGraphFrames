Project P1Name: Isha Bobra, unity id: ibobraName: Pranav Kulkarni, unity id: pkulkar5


How to run the scripts:

Note: `pyFiles` is defined in each of the scripts as an argument to SparkContext. Please change it your appropriate location before running.

degree.py

$SPARK_HOME/bin/spark-submit --jars /home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar degree.py stanford_graphs/amazon.graph.small


centrality.py

$SPARK_HOME/bin/spark-submit --jars /home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar centrality.py


articulation.py --> this script takes a long time. Approximately 45 mins to run

$SPARK_HOME/bin/spark-submit --driver-memory 4g --jars /home/isha/spark-1.6.2-bin-hadoop2.6/graphframes-0.1.0-spark1.6.jar articulation.py 9_11_edgelist.txt
