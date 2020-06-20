import sys
import json
import time
from pyspark import SparkContext, SparkConf
import math

def cosine_similarity(s1, s2):
    denominator = len(s1) * len(s2)
    if denominator == 0:
        return 0.0
    denominator = math.sqrt(len(s1)) * math.sqrt(len(s2))
    return len(s1.intersection(s2)) / float(denominator)

def main(argv):
    assert len(argv) == 3, "Script accepts 3 arguments <test_file><model_file><output_file>"

    # Unpack arguments
    test_file, model_file, output_file = argv

    config = SparkConf() \
        .setMaster("local[*]") \
        .setAppName("Task2prediction") \
        .set("spark.executor.memory", "4g") \
        .set("spark.driver.memory", "4g")

    sc = SparkContext(conf=config).getOrCreate()

    lines = sc.textFile(model_file).map(json.loads).cache()

    business_tokens = lines.filter(lambda x: x["description"] == "business_tokens") \
        .map(lambda x: (x["id"], x["token"])) \
        .collectAsMap()

    user_tokens = lines.filter(lambda x: x["description"] == "user_tokens") \
        .map(lambda x: (x["id"], x["token"])) \
        .collectAsMap()

    business_profile = lines.filter(lambda x: x["description"] == "business_profile") \
        .map(lambda x: (x["id"], x["profile"])) \
        .collectAsMap()

    user_profile = lines.filter(lambda x: x["description"] == "user_profile") \
        .map(lambda x: (x["id"], x["profile"])) \
        .collectAsMap()

    predictions = sc.textFile(test_file).map(json.loads) \
        .map(lambda x: ((x["user_id"], x["business_id"]), (user_tokens.get(x["user_id"]), business_tokens.get(x["business_id"])))) \
        .filter(lambda x: x[1][0] != None and x[1][1] != None) \
        .mapValues(lambda x: cosine_similarity(set(user_profile[x[0]]), set(business_profile[x[1]]))) \
        .filter(lambda x: x[1] >= 0.01) \
        .collect()

    with open(output_file, "w+") as file:
        for line in predictions:
            value = {"user_id": line[0][0], "business_id": line[0][1], "sim": line[1]}
            file.writelines(json.dumps(value) + "\n")
        file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
