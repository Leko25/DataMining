import json
import os
import sys
import time

from pyspark import SparkConf, SparkContext


def isValidFiles(input_file):
    if not os.path.isfile(input_file):
        return False
    return True


def default(sc, input_file, n_partitions, n_arg):
    start = time.time()

    lines = sc.textFile(input_file).coalesce(n_partitions).cache()

    rdd = lines.map(json.loads).map(lambda x: (x["business_id"], 1))

    num_partitions = rdd.getNumPartitions()

    n_items = rdd.glom().map(len).collect()

    business_sum = rdd.reduceByKey(lambda x, y: x + y)

    popular_businesses = business_sum.filter(lambda x: x[1] > n_arg)

    results = popular_businesses.collect()

    # Convert list of tuples to list of lists
    results = list(map(list, results))

    end = time.time()

    print("Default Time ----> ", end - start)

    return (num_partitions, n_items, results)

def customized(sc, input_file, n_partitions, n_arg):
    start = time.time()

    lines = sc.textFile(input_file)

    rdd = lines.map(json.loads).map(lambda x: (x["business_id"], 1)).partitionBy(n_partitions, lambda x: hash(x)).cache()

    num_partitions = rdd.getNumPartitions()

    n_items = rdd.glom().map(len).collect()

    business_sum = rdd.reduceByKey(lambda x, y: x + y, 8)

    popular_businesses = business_sum.filter(lambda x: x[1] > n_arg)

    results = popular_businesses.collect()
    results = list(map(list, results))

    end = time.time()

    print('Cutomized Time ----> ', end - start)

    return (num_partitions, n_items, results)


def main(argv):
    assert len(argv) == 5, "Script takes 5 arguments"

    # Unpack arguments
    input_file, output_file, partition_type, n_partitions, n_arg = argv

    # Check for valid file paths
    if not isValidFiles(input_file):
        print("Invalid path arguments")
        return

    n_arg = int(n_arg)
    n_partitions = int(n_partitions)
    out_dict = {}

    # Create Spark configuration and context
    conf = SparkConf().setMaster("local[*]").setAppName("Task3")
    sc = SparkContext(conf=conf)

    if partition_type == "default":
        (num_partitions, n_items, results) = default(sc, input_file, n_partitions, n_arg)
        out_dict["n_partitions"] = num_partitions
        out_dict["n_items"] = n_items
        out_dict["result"] = results
    else:
        (num_partitions, n_items, results) = customized(sc, input_file, n_partitions, n_arg)
        out_dict["n_partitions"] = num_partitions
        out_dict["n_items"] = n_items
        out_dict["result"] = results

    with open(output_file, "w") as file:
        json.dump(out_dict, file)

if __name__ == "__main__":
    main(sys.argv[1:])
