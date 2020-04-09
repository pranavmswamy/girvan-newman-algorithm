from pyspark import SparkContext
from pyspark.sql import SparkSession
from graphframes import *
import os
from sys import argv
from time import time

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")

sc = SparkContext()
start_time = time()
sc.setLogLevel("ERROR")
spark = SparkSession(sparkContext=sc)

power_file_rdd = sc.textFile(str(argv[1])).map(lambda x: (x.split(" ")[0], x.split(" ")[1]))
edges_rdd = power_file_rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).distinct()
vertices_rdd = power_file_rdd.flatMap(lambda x: x).distinct().map(lambda x: (x,))
v_list = vertices_rdd.collect()
e_list = edges_rdd.collect()

v = spark.createDataFrame(v_list, ['id'])
e = spark.createDataFrame(e_list, ['src', 'dst'])

g = GraphFrame(v, e)

label = g.labelPropagation(maxIter=5)

clusters_rdd = label.rdd
clusters_rdd = clusters_rdd.map(lambda i: (i['label'], i['id'])).groupByKey().mapValues(lambda l: sorted(list(l)))
clusters_list = clusters_rdd.collect()
clusters_list.sort(key=lambda x: (len(x[1]), x[1][0]))

with open(str(argv[2]), "w") as file:
    for community in clusters_list:
        community_list = community[1]
        if len(community_list) == 1:
            file.write(str("'" + community_list[0] + "'\n"))
        else:
            file.write(str("'" + community_list[0] + "'"))
            for num in range(1,len(community_list)):
                file.write(", ")
                file.write(str("'" + community_list[num]+ "'"))
            file.write("\n")
    file.close()

print("Time taken: ", time()-start_time,"s")