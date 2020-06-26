import json
import sys
import binascii
import random
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from datetime import datetime
import time

random.seed(25)

# Globals
WINDOW_LEN = 30 # seconds
SLIDING_INTERVAL = 10 # seconds
HOST = "localhost"
NUM_HASH = 12
output_file = None

def hash_funcs():
    p = 2**35 - 355
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    m = 4293517
    return lambda x: ((a * x + b) % p) % m


def fm(x):
    curr_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")

    cities = list(set(x.collect()))

    hash_list = [hash_funcs() for _ in range(NUM_HASH)]

    results = []

    for i in range(NUM_HASH):
        R = -float('inf')
        for city in cities:
            city_hash = int(binascii.hexlify(city.encode("utf8")), 16)
            hash_func = hash_list[i]

            hash_val = format(hash_func(city_hash), 'b').zfill(20)

            rho_y = 0

            if hash_val != 0:
                rho_y = len(str(hash_val)) - len(str(hash_val).rstrip("0"))
            R = max(rho_y, R)
        results.append(2**R)

    divider = NUM_HASH // 2
    s1 = results[:divider]
    s2 = results[divider:]
    means = [sum(s1) / NUM_HASH, sum(s2) / NUM_HASH]
    median = sum(means)/2

    with open(output_file, "a", encoding="utf-8") as file:
        file.write(curr_time + "," + str(len(cities)) + "," + str(int(median)) + "\n")
        file.close()

def main(argv):

    assert len(argv) == 2, "Script takes 2 arguments <PORT><output_file>"
    global output_file

    # Unpack arguments
    port, output_file = argv
    port = int(port)

    config = SparkConf().setMaster("local[*]") \
                      .setAppName("Task2") \
                      .set("spark.executor.memory", "4g") \
                      .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=config).getOrCreate()
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 5)

    conn = ssc.socketTextStream(HOST, port)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write("Time,Ground Truth,Estimation\n")
        file.close()

    stream = conn.window(WINDOW_LEN, SLIDING_INTERVAL) \
                 .map(json.loads) \
                 .map(lambda x: x.get("city")) \
                 .filter(lambda x: x != None and x != "") \
                 .foreachRDD(fm)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
