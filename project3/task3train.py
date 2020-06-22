import json
import time
from pyspark import SparkContext, SparkConf
import math
import sys

def get_intersect(s1, s2):
    return len(set(s1.keys()).intersection(set(s2.keys())))

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
        with open(model_file, "w+") as file:
            for line in results:
                file.writelines(json.dumps({"b1": tokens_business[line[0][0]], "b2": tokens_business[line[0][1]], "sim": line[1]}) + "\n")
            file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
