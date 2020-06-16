import os
import sys
import time
import logging
from collections import defaultdict
from itertools import combinations
import random
import json
from functools import reduce
from pyspark import SparkContext, SparkConf

# TODO comment import and lines below
# import findspark
# findspark.init("/Users/chukuemekaogudu/Documents/Dev-Spark-Apache/Apache-Spark/spark-2.4.5-bin-hadoop2.7")


# Set random seed
random.seed(25)

# Globals
THRESHOLD = 0.05
BANDS = 500
NUM_BUCKETS = 1000

def min_hash_func(idx):
    p = 2**35 - 365
    a = random.randint(1, p - 1)
    b = random.randint(217, p - 1)
    m = 42949
    return lambda x: ((a * x + b * idx) % p) % m

def get_hash(row, hash_funcs):
    hash_vals = [hash_func(row) for hash_func in hash_funcs]
    return hash_vals

def get_user_hash(pair):
    hash_val = pair[0]
    users = pair[1]
    return [(user, hash_val) for user in users]

def min_hash(h1, h2):
    signature = [min(v1, v2) for v1, v2 in zip(h1, h2)]
    return signature

def lsh_hash(idx):
    p = 2**75 - 545
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    m = 72935
    return lambda x: ((a * x + b * idx) % p) % m

def generate_bands(signature):
    bands = []
    length = len(signature)
    window = length//BANDS
    idx = 1
    for i in range(0, length, window):
        start = i
        bands.append((idx, signature[i: i + window]))
        idx += 1
    return bands

def group_bands(pairs):
    business_idx = pairs[0]
    group = []

    for band_pair in pairs[1]:
        group.append((band_pair[0], (business_idx, band_pair[1])))
    return group

def lsh(bands, lsh_hash_funcs):
    band_id = bands[0]
    pairs = bands[1]

    hash_table = defaultdict(list)
    for pair in pairs:
        business_id = pair[0]
        hash_sum = hash(tuple(pair[1]))
        hash_func = lsh_hash_funcs[band_id - 1]
        hash_val = hash_func(hash_sum)
        hash_table[hash_val].append(business_id)

    results = [v for _, v in hash_table.items()]
    return (band_id, results)

def jaccard(s1, s2):
    return len(set(s1).intersection(set(s2)))/len(set(s1).union(set(s2)))

def compute_similarity(pairs, business_user_dict, reversed_b_dict):
    candidate_pairs = []
    for pair in pairs:
        s1 = business_user_dict.get(pair[0], set())
        s2 = business_user_dict.get(pair[1], set())
        sim = jaccard(s1, s2)

        if sim >= THRESHOLD:
            candidate_pairs.append({
                "b1": reversed_b_dict.get(pair[0]),
                "b2": reversed_b_dict.get(pair[1]),
                "sim": sim})
    return candidate_pairs
def main(argv):
    assert len(argv) == 2, "Script takes 2 arguments <input_file_path><output_file_path"

    # Unpack arguments
    input_file, output_file = argv

    # Set Logging
    logging.getLogger("org").setLevel(logging.ERROR)

    config = SparkConf().setMaster("local[*]") \
            .setAppName("Task1") \
            .set("spark.executor.memory", "4g") \
            .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=config).getOrCreate()

    lines = sc.textFile(input_file)

    rdd = lines.map(json.loads) \
            .map(lambda x: (x["user_id"], x["business_id"])).cache()

    business_map = rdd.map(lambda x: x[1]) \
            .distinct() \
            .zipWithIndex() \
            .cache()
    b_dict = business_map.collectAsMap()
    reversed_b_dict = business_map.map(lambda x: (x[1], x[0])).collectAsMap()

    user_dict = rdd.map(lambda x: x[0]) \
            .distinct() \
            .zipWithIndex() \
            .collectAsMap()

    user_business_rdd = rdd.map(lambda x: (user_dict[x[0]], b_dict[x[1]])) \
            .groupByKey() \
            .mapValues(lambda x: list(set(x))) \
            .cache()

    business_user_dict = rdd.map(lambda x: (b_dict[x[1]], user_dict[x[0]])) \
            .groupByKey() \
            .mapValues(lambda x: list(set(x))) \
            .collectAsMap()

    # MinHash Implementation
    hash_funcs = [min_hash_func(i) for i in range(NUM_BUCKETS)]

    hash_rdd = user_business_rdd.map(lambda x: (x[0], get_hash(x[0], hash_funcs)))

    joined_hash_rdd = hash_rdd.join(user_business_rdd).partitionBy(7, lambda x: hash(x))

    signature_mat = joined_hash_rdd.map(lambda x: get_user_hash(x[1])) \
            .flatMap(lambda x: x) \
            .reduceByKey(lambda h1, h2: min_hash(h1, h2)) \
            .cache()

    # LSH Implementation
    lsh_hash_funcs = [lsh_hash(i) for i in range(BANDS)]

    candidates = signature_mat.map(lambda x: (x[0], generate_bands(x[1]))) \
            .map(group_bands) \
            .flatMap(lambda x: x) \
            .groupByKey() \
            .map(lambda x: lsh(x, lsh_hash_funcs)) \
            .flatMap(lambda x: x[1]) \
            .filter(lambda x: len(x) > 1) \
                    .flatMap(lambda pairs: [pair for pair in combinations(pairs, 2)]) \
                    .distinct() \
                    .collect()

    print("Total Candidate pairs ---------------> ", len(candidates))

    results = compute_similarity(candidates, business_user_dict, reversed_b_dict)
    print("Accuracy -----------------> " + str(len(results)//59435 * 100) + " %")

    # Wirte to file
    with open(output_file, "w+") as file:
        for line in results:
            file.writelines(json.dumps(line) + "\n")
        file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    end = time.time()
    print("Duration ", end - start)
