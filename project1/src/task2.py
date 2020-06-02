import json
from pyspark import SparkContext, SparkConf
import os
import sys
import logging
import time

def isValidFiles(review_file, business_file):
    if not os.path.isfile(review_file) or not os.path.isfile(business_file): 
        return False
    return True


def loadBusiness(jsonObj):
    categories = jsonObj.get("categories")
    categories_list = None
    if categories == None or categories == "":
        categories_list = []
    else:
        categories = categories.split(",")
        categories_list = [category.strip() for category in categories]
    return (jsonObj["business_id"], categories_list)


def loadReviews(jsonObj):
    return (jsonObj["business_id"], (float(jsonObj["stars"]), 1))


def mapCategory(values):
    categories_list = values[0]
    stars_count = values[1]
    categoriesMap = [(value, stars_count) for value in categories_list]
    return categoriesMap


def loadBusinessJson(business_file):
    id_cat = {}
    cat_count = {}
    with open(business_file, "r") as file:
        lines = file.readlines()

        for line in lines:
            jsonObj = json.loads(line)

            categories = jsonObj.get("categories")

            if categories != None and categories != "":
                categories = categories.split(",")
                categories_list = [category.strip() for category in categories]
                id_cat[jsonObj["business_id"]] = categories_list

                for category in categories_list:
                    if cat_count.get(category) == None:
                        cat_count[category] = [0.0, 0]
    return id_cat, cat_count


def mapBusinessReviews(review_file, id_cat, cat_count):
    file = open(review_file, "r")

    while True:
        line = file.readline()

        if not  line:
            break

        jsonObj = json.loads(line)

        categories_list = id_cat.get(jsonObj["business_id"])

        if categories_list != None:
            for category in categories_list:
                cat_count[category][0] += float(jsonObj["stars"])
                cat_count[category][1] += 1

    return cat_count


def get_top_avg(results, n_arg):
    avg_cat_dict = {}

    for pair in results:
        if avg_cat_dict.get(pair[1]) == None:
            avg_cat_dict[pair[1]] = [pair[0]]
        else:
            avg_cat_dict[pair[1]].append(pair[0])

    top_avg_stars = []

    for star, cateogories in avg_cat_dict.items():
        if n_arg == 0:
            break
        
        cateogories = sorted(cateogories)

        for category in cateogories:
            if n_arg == 0:
                break

            top_avg_stars.append([category, star])
            n_arg -= 1

    return top_avg_stars



def main(argv):
    assert len(argv) == 5, "Script takes 5 arguments"

    # Unpack arguments 
    review_file, business_file, output_file, if_spark, n_arg = argv

    # Check for valid file paths
    if not isValidFiles(review_file, business_file):
        print("Invalid path arguments")
        return

    n_arg = int(n_arg)
    out_dict = {}

    if if_spark == "spark":
        start = time.time()

        # Get Error Logs
        logging.getLogger("org").setLevel(logging.ERROR)

        conf = SparkConf().setMaster("local[*]").setAppName("Task2")
        sc = SparkContext(conf=conf).getOrCreate()

        # Create BusinessRDD and ReviewsRDD
        businessJson = sc.textFile(business_file)
        reviewJson = sc.textFile(review_file)

        businessRDD = businessJson.map(json.loads).map(lambda x: loadBusiness(x))
        reviewsRDD = reviewJson.map(json.loads).map(lambda x: loadReviews(x))

        # Natural Join Business and Reviews
        business_reviews = businessRDD.join(reviewsRDD).cache()

        category_stars = business_reviews.filter(lambda x: len(x[1][0]) != 0) \
                                         .map(lambda x: mapCategory(x[1])) \
                                         .flatMap(lambda x: x) \
                                         .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).cache()

        # Compute the average
        avg_stars = category_stars.mapValues(lambda x: x [0]/x[1]).sortBy(lambda x: x[1], ascending=False)

        results = avg_stars.collect()

        top_avg_stars = get_top_avg(results, n_arg)

        out_dict["result"] = top_avg_stars

        end = time.time()

        print("Task 2 ", end - start)

    else:
        start = time.time()

        id_cat, cat_count = loadBusinessJson(business_file)
        
        cat_count = mapBusinessReviews(review_file, id_cat, cat_count)

        del id_cat # free up memory

        avg_cat = {}

        for category, star_count in cat_count.items():
            try:
                avg_cat[category] = star_count[0]/star_count[1]
            except ZeroDivisionError as e:
                avg_cat[category] = 0.0

        del cat_count # free up memory

        # sort average catgory rating --> list of tuples
        avg_cat = sorted(avg_cat.items(), key=lambda kv: kv[1], reverse=True)
        
        top_avg_stars = get_top_avg(avg_cat, n_arg)

        out_dict["result"] = top_avg_stars

        end = time.time()

        print("Task 2 ", end - start)

    with open(output_file, "w") as file:
        json.dump(out_dict, file)
        

if __name__ == "__main__":
    main(sys.argv[1:])