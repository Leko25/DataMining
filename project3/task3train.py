import json
import logging
import math
import random
import sys
import time
from collections import defaultdict
from functools import reduce
from itertools import combinations

from pyspark import SparkConf, SparkContext

random.seed(25)

# Globals
NUM_BUCKETS = 1000
BANDS = 500

def min_hash_func(idx):
    p = 2**35 - 365
    a = random.randint(1, p - 1)
    b = random.randint(217, p - 1)
    m = 4294975
    return lambda x: ((a * x + b * idx) % p) % m

def lsh_hash(idx):
    p = 2**75 - 545
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    m = 7293513
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

def generate_bands(signature):
    bands = []
    length = len(signature)
    window = length // BANDS
    idx = 1
    for i in range(0, length, window):
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

def get_intersect(s1, s2):
    return len(set(s1.keys()).intersection(set(s2.keys())))


def jaccard(s1, s2):
    b1 = set(s1.keys())
    b2 = set(s2.keys())
    return len(b1.intersection(b2)) / len(b1.union(b2))

def pearson_correlation(ri, rj):
    ru = list(set(ri.keys()).intersection(set(rj.keys())))
    avg_ri = sum([ri[rui] for rui in ru]) / len(ri)
    avg_rj = sum([rj[ruj] for ruj in ru]) /len(rj)
    
    i,j,numerator = 0.0,0.0,0.0
    
    for k in ru:
        i += (ri[k] - avg_ri)**2
        j += (rj[k] - avg_rj)**2
        numerator += (ri[k] - avg_ri) * (rj[k] - avg_rj)
    denominator = math.sqrt(i) * math.sqrt(j)
    if denominator == 0.0:
        return 0.0
    return numerator / denominator

def main(argv):
    assert len(argv) == 3, "Script takes 3 arguments <train_file><model_file><cf_type>"
    
    # Unpack arguments
    train_file, model_file, cf_type = argv
    
    config = SparkConf().setMaster("local[*]") \
                        .setAppName("Task3train") \
                        .set("spark.executor.memory", "4g") \
                        .set("spark.driver.memory", "4g")
                
    sc = SparkContext(conf=config).getOrCreate()
    
    sc.setLogLevel("ERROR")
    
    if cf_type == "item_based":
        
        lines = sc.textFile(train_file).map(json.loads).cache()
        
        business_tokens = lines.map(lambda x: x["business_id"]).distinct().zipWithIndex().collectAsMap()
        tokens_business = {v: k for k, v in business_tokens.items()}
        
        rdd = lines.map(lambda  x: (business_tokens[x["business_id"]], (x["user_id"], x["stars"]))) \
                   .groupByKey().filter(lambda x: len(x[1]) >= 3) \
                   .mapValues(dict) \
                   .cache()
                    
        tokens_rdd = rdd.map(lambda x: x[0])
        
        rdd_dict = rdd.collectAsMap()
        
        results = tokens_rdd.cartesian(tokens_rdd) \
                               .filter(lambda x: x[0] < x[1]) \
                               .filter(lambda x: get_intersect(rdd_dict[x[0]], rdd_dict[x[1]]) >= 3) \
                               .map(lambda x: ((x[0], x[1]), pearson_correlation(rdd_dict[x[0]], rdd_dict[x[1]]))) \
                               .filter(lambda x: x[1] > 0.0).collect()
                        
        print("Number of candidates --------------> ", len(results))
        with open(model_file, "w+") as file:
            for line in results:
                file.writelines(json.dumps({"b1": tokens_business[line[0][0]], "b2": tokens_business[line[0][1]], "sim": line[1]}) + "\n")
            file.close()
    else:
        lines = sc.textFile(train_file).map(json.loads).cache()
        
        business_tokens = lines.map(lambda x: x["business_id"]).distinct().zipWithIndex().collectAsMap()
        tokens_business = {v: k for k, v in business_tokens.items()}
        
        user_tokens = lines.map(lambda x: x["user_id"]).distinct().zipWithIndex().collectAsMap()
        tokens_user = {v: k for k, v in user_tokens.items()}
        
        business_users = lines.map(lambda x: (business_tokens[x["business_id"]], user_tokens[x["user_id"]])) \
                              .groupByKey() \
                              .filter(lambda x: len(x[1]) >= 3) \
                              .mapValues(list).cache()
                    
        users_business = lines.map(lambda x: (user_tokens[x["user_id"]], (business_tokens[x["business_id"]], x["stars"]))) \
                              .groupByKey() \
                              .filter(lambda x: len(x[1]) >= 3) \
                              .mapValues(dict) \
                              .collectAsMap()
                        
        # MinHash
        hash_funcs = [min_hash_func(i) for i in range(NUM_BUCKETS)]
        
        hash_rdd = business_users.map(lambda x: (x[0], get_hash(x[0], hash_funcs)))
        
        joined_hash_rdd = hash_rdd.join(business_users).partitionBy(7, lambda x: hash(x) % 7)
        
        signature_mat = joined_hash_rdd.map(lambda x: get_user_hash(x[1])) \
                                       .flatMap(lambda x: x) \
                                       .reduceByKey(lambda h1, h2: min_hash(h1, h2))
        
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
                             .filter(lambda x: users_business.get(x[0]) != None and users_business.get(x[1]) != None) \
                             .filter(lambda x: get_intersect(users_business[x[0]], users_business[x[1]]) >= 3) \
                             .filter(lambda x: jaccard(users_business[x[0]], users_business[x[1]]) >= 0.01) \
                             .map(lambda x: ((x[0], x[1]), pearson_correlation(users_business[x[0]], users_business[x[1]]))) \
                             .filter(lambda x: x[1] > 0.0).collect()        
        print("Number of candidates -----------------> ", len(candidates))
        with open(model_file, "w+") as file:
            for line in candidates:
                file.writelines(json.dumps({"u1": tokens_user[line[0][0]], "u2": tokens_user[line[0][1]], "sim": line[1]}) + "\n")
            file.close()
                
            
if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
