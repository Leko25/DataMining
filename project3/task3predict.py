import json
import time
from pyspark import SparkContext, SparkConf
import math
import sys
import os

# Globals
N = 3

def item_prediction(data, avg_dict, id_list, sim_dict):
    target = data[0]
    if target not in id_list:
        target = "UNK"
    pairs = data[1]

    values = []
    for pair in pairs:
        sim = 0
        if sim_dict.get(tuple((pair[0], target))) != None:
            sim = sim_dict.get(tuple((pair[0], target)))
        if sim_dict.get(tuple((target, pair[0]))) != None:
            sim = sim_dict.get(tuple((target, pair[0])))
        values.append((sim, pair[1]))

    values_sorted = sorted(values, key=lambda kv: kv[0], reverse=True)
    neighborhood = values_sorted[:N]

    num = 0
    for v in neighborhood:
        num += v[0] * v[1]

    denum = 0
    for v in neighborhood:
        denum += abs(v[0])

    if denum == 0 or num == 0:
        return (target, avg_dict.get(target, avg_dict["UNK"]))
    return (target, num/denum)

def user_prediction(data, avg_dict, id_list, sim_dict):
    target = data[0]
    if target not in id_list:
        target = "UNK"
    pairs = data[1]

    values = []
    for pair in pairs:
        sim = 0
        if sim_dict.get(tuple((pair[0], target))) != None:
            sim = sim_dict.get(tuple((pair[0], target)))
        if sim_dict.get(tuple((target, pair[0]))) != None:
            sim = sim_dict.get(tuple((target, pair[0])))
        values.append((sim, pair[1], avg_dict.get(pair[0], avg_dict["UNK"])))

    num = 0
    for v in values:
        num += (v[1] - v[2]) * v[0]

    denum = 0
    for v in values:
        denum += abs(v[0])
    if denum == 0 or num == 0:
        return (target, avg_dict.get(target, avg_dict["UNK"]))
    result = (num / float(denum)) + avg_dict.get(target, avg_dict["UNK"])
    return (target, result)

def main(argv):
    train_file, test_file, model_file, output_file, cf_type = argv

    config = SparkConf().setMaster("local[*]") \
                        .setAppName("Task3predict") \
                        .set("spark.executor.memory", "4g") \
                        .set("spark.driver.memory", "4g")

    sc = SparkContext(conf=config).getOrCreate()
    sc.setLogLevel("ERROR")

    ext = os.path.dirname(train_file)

    if cf_type == "item_based":
        model_dict = sc.textFile(model_file) \
                       .map(json.loads) \
                       .map(lambda x: ((x["b1"], x["b2"]), x["sim"])) \
                       .collectAsMap()

        test_rdd = sc.textFile(test_file) \
                     .map(json.loads) \
                     .map(lambda x: (x["user_id"], x["business_id"]))

        avg_dict = sc.textFile(os.path.join(ext, "business_avg.json")) \
                     .map(json.loads) \
                     .map(dict) \
                     .flatMap(lambda x: x.items()) \
                     .collectAsMap()

        train_rdd = sc.textFile(train_file).map(json.loads).cache()

        unique_ids = train_rdd.map(lambda x: x["business_id"]).distinct().collect()

        user_business = train_rdd.map(lambda x: (x["user_id"], (x["business_id"], x["stars"]))) \
                                 .groupByKey() \
                                 .mapValues(lambda x: list(set(x)))
        results = test_rdd.leftOuterJoin(user_business) \
                          .mapValues(lambda x: item_prediction(x, avg_dict, unique_ids, model_dict)) \
                          .collect()
        with open(output_file, "w+") as file:
            for line in results:
                file.writelines(json.dumps({"user_id": line[0], "business_id": line[1][0], "stars": line[1][1]}) + "\n")
            file.close()

    else:
        model_dict = sc.textFile(model_file) \
                       .map(json.loads) \
                       .map(lambda x: ((x["u1"], x["u2"]), x["sim"])) \
                       .collectAsMap()

        test_rdd = sc.textFile(test_file) \
                     .map(json.loads) \
                     .map(lambda x: (x["business_id"], x["user_id"]))

        avg_dict = sc.textFile(os.path.join(ext, "user_avg.json")) \
                     .map(json.loads) \
                     .map(dict) \
                     .flatMap(lambda x: x.items()) \
                     .collectAsMap()

        train_rdd = sc.textFile(train_file).map(json.loads).cache()

        unique_ids = train_rdd.map(lambda x: x["user_id"]).distinct().collect()

        business_user = train_rdd.map(lambda x: (x["business_id"], (x["user_id"], x["stars"]))) \
                                 .groupByKey() \
                                 .mapValues(lambda x: list(set(x)))

        results = test_rdd.leftOuterJoin(business_user) \
                          .filter(lambda x: x[1][1] != None) \
                          .mapValues(lambda x: user_prediction(x, avg_dict, unique_ids, model_dict)) \
                          .collect()
        with open(output_file, "w+") as file:
            for line in results:
                file.writelines(json.dumps({"user_id": line[1][0], "business_id": line[0], "stars": line[1][1]}) + "\n")
            file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
