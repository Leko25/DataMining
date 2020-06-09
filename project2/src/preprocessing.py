import sys
import time
from pyspark import SparkContext, SparkConf
import logging
import json

def loadBusiness(josnObj):
    state = ""
    if josnObj.get("state"):
        state = josnObj.get("state")
    return (josnObj["business_id"], state)

def loadReviews(josnObj):
    return (josnObj["user_id"], josnObj["business_id"])


def toCSV(data):
    return ','.join(str(d) for d in data)

def main(argv):
    assert len(argv) == 3, "Script takes 3 arguments"

    # Unpack arguments
    review_file, business_file, output_file = argv

    logging.getLogger("org").setLevel(logging.ERROR)

    config = SparkConf().setMaster("local[*]").setAppName("taks1")
    sc = SparkContext(conf=config).getOrCreate()

    businessJSON = sc.textFile(business_file)
    reviewJSON = sc.textFile(review_file)

    businessRDD = businessJSON.map(json.loads) \
                              .map(lambda x: loadBusiness(x)) \
                              .filter(lambda x: x[1] == "NV") \
                              .map(lambda x: x[0])

    business = businessRDD.collect()

    reviewsRDD = reviewJSON.map(json.loads).map(lambda x: loadReviews(x))

    user_business_id = reviewsRDD.filter(lambda x: x[1] in business).map(toCSV).collect()

    with open(output_file, "w") as file:
        file.write("user_id,business_id\n")
        for line in user_business_id:
            file.write(line)
            file.write("\n")


if __name__ == "__main__":
    main(sys.argv[1:])