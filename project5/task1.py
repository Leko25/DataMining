import json
from pyspark import SparkContext, SparkConf
import time
import random
import binascii
import sys

random.seed(25)

# GLOBALS
NUM_HASH = 7

def hash_funcs():
    p = 2**35 - 355
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    m = 8351
    return lambda x: ((a * x + b) % p) % m


def main(argv):
    assert len(argv) == 3

    first_path, second_path, output_path = argv

    config = SparkConf().setMaster("local[*]") \
                        .set("spark.executor.memory", "4g") \
                        .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=config).getOrCreate()
    sc.setLogLevel("ERROR")

    first = sc.textFile(first_path) \
              .map(json.loads) \
              .map(lambda x: x.get("city")) \
              .filter(lambda x: x != None and x != "") \
              .distinct() \
              .map(lambda x: int(binascii.hexlify(x.encode("utf8")), 16))

    bit_vector = [0] * 8351
    hash_list = [hash_funcs() for _ in range(NUM_HASH)]

    hash_pos = first.map(lambda x: [func(x) for func in hash_list]) \
                    .flatMap(lambda x: x) \
                    .distinct() \
                    .collect()
    for pos in hash_pos:
        bit_vector[pos] = 1

    predictions = []

    second = sc.textFile(second_path) \
               .map(json.loads) \
               .map(lambda x: x.get("city")) \
               .collect()
    for city in second:
        if city != None and city != "":
            city = int(binascii.hexlify(city.encode("utf8")), 16)
            bit_val = [bit_vector[func(city)] for func in hash_list]
            if sum(bit_val) == len(bit_val):
                predictions.append("1")
            else:
                predictions.append("0")
        else:
            predictions.append("0")

    results = " ".join(predictions)

    with open(output_path, "w") as file:
        file.write(results)

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
