import sys
import time
from pyspark import SparkContext, SparkConf
from graphframes import GraphFrame
import os
from itertools import combinations
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

def get_intersection(s1, s2):
    return len(set(s1).intersection(set(s2)))


def main(argv):
    assert len(argv) == 3, "Script takes 3 arguments <filter_threshold><input_file><community_output_file>"

    filter_threshold, input_file, output_file = argv

    filter_threshold = int(filter_threshold)

    config = SparkConf().setMaster("local[*]") \
                        .setAppName("Task2") \
                        .set("spark.executor.memory", "4g") \
                        .set("spark.driver.memory", "4g")

    sc = SparkContext(conf=config).getOrCreate()
    spark = SparkSession(sc)
    sc.setLogLevel("ERROR")

    lines = sc.textFile(input_file)
    header = lines.first()

    rdd_dict = lines.filter(lambda x: x != header) \
               .map(lambda x: (x.split(',')[0], x.split(',')[1])) \
               .groupByKey().collectAsMap()

    user_pairs = list(combinations(rdd_dict.keys(), 2))

    edges_rdd = sc.parallelize(user_pairs) \
                       .map(lambda x: (x[0], x[1])) \
                       .filter(lambda x: get_intersection(rdd_dict[x[0]], rdd_dict[x[1]]) >= filter_threshold) \
                       .cache()

    nodes_df = edges_rdd.flatMap(lambda x: x).distinct().map(lambda x: (x,)).toDF(["id"])

    edges_df = edges_rdd.toDF(["src", "dst"])

    gf = GraphFrame(nodes_df, edges_df)

    communities_rdd = gf.labelPropagation(maxIter=5).rdd.coalesce(1)

    communities = communities_rdd.map(lambda x: (x[1], x[0])) \
                                 .groupByKey() \
                                 .map(lambda x: sorted(list(x[1]))) \
                                 .sortBy(lambda x: (len(x), x)) \
                                 .collect()

    with open(output_file, "w+") as file:
        for community in communities:
            value = str(community)[1:-1]
            file.writelines(value + "\n")
        file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
