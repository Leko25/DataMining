import json
import logging
import os
import re
import string
import sys
from datetime import datetime

from pyspark import SparkConf, SparkContext
import time

stopwords = None

def isValidFiles(input_file, stop_words_file):
    if not os.path.isfile(input_file) or not os.path.isfile(stop_words_file): 
        return False
    return True

def loadStopWords(file_path):
    data = None
    with open(file_path, "rb") as file:
        data = file.read()
    return data.decode("utf-8")

def mapJsonObj(jsonObj):
    year = datetime.strptime(jsonObj["date"], "%Y-%m-%d %H:%M:%S").year

    words = jsonObj["text"].translate(str.maketrans('', '', string.punctuation))
    words = words.split()
    pattern = "[a-zA-Z]+"

    words_dict = {}
    for word in words:
        word = word.lower()
        if re.match(pattern, word) and word not in stopwords:
            if word not in words_dict:
                words_dict[word] = 1
            else:
                words_dict[word] += 1
    words_list = list(words_dict.items())
    return (
        (year, 1), 
        (jsonObj["review_id"], 1), 
        (jsonObj["user_id"], 1),
        words_list
        )

def get_top_users(results):
    avg_cat_dict = {}

    for pair in results:
        if avg_cat_dict.get(pair[1]) == None:
            avg_cat_dict[pair[1]] = [pair[0]]
        else:
            avg_cat_dict[pair[1]].append(pair[0])

    top_avg_stars = []

    for star, cateogories in avg_cat_dict.items():
        cateogories = sorted(cateogories)

        for category in cateogories:
            top_avg_stars.append([category, star])

    return top_avg_stars


def main(argv):
    global stopwords

    assert len(argv) == 6, "Script takes 6 arguments"

    # Unpack arguments
    input_file, output_file, stop_words_file, y_arg, m_arg, n_arg = argv

    # Check for valid file paths
    if not isValidFiles(input_file, stop_words_file):
        print ("Invalid file path argument")
        return

    y_arg = int(y_arg)
    m_arg = int(m_arg)
    n_arg = int(n_arg)

    start = time.time()

    # Get Error Logs
    logging.getLogger("org").setLevel(logging.ERROR)

    conf = SparkConf().setMaster("local[*]").setAppName("Task1")
    sc = SparkContext(conf=conf).getOrCreate()

    # Load Stopwords into main memory
    stopwords = loadStopWords(stop_words_file).split()

    # Load RDD
    lines = sc.textFile(input_file)
    rdd = lines.map(json.loads).map(lambda x: mapJsonObj(x)).cache()
    
    out_dict = {}

    # Determine the total reviews in the file
    total_reviews = rdd.count()
    out_dict["A"] = total_reviews

    # Determine the reviews in a given year
    reviews_y = rdd.map(lambda x: x[0]) \
                   .filter(lambda x: x[0] == y_arg) \
                   .reduceByKey(lambda x, y: x + y)

    out_dict["B"] = reviews_y.collect()[0][1]

    # Determine the number of distinct users who have written the reviews
    unique_users = rdd.map(lambda x: x[2][0]).distinct().count()
    out_dict["C"] = unique_users

    # Determine the top m users who have the largest number of reviews and their count
    user_reviews = rdd.map(lambda x: x[2]) \
                      .reduceByKey(lambda x, y: x + y) \
                      .sortBy(lambda x: x[1], ascending=False)
    #top_user_reviews = user_reviews.take(m_arg)

    user_reviews_list = user_reviews.take(m_arg)
    top_user_reviews = get_top_users(user_reviews_list)


    # Convert list of tuples into list of lists

    out_dict["D"] = top_user_reviews

    # Determine the top n frequent words in the review text
    top_words_list = []
    top_words = rdd.map(lambda x: x[3]) \
                   .flatMap(lambda x: x) \
                   .reduceByKey(lambda x, y: x + y) \
                   .sortBy(lambda x: x[1], ascending=False)
    top_n_words = top_words.take(n_arg)
    out_dict["E"] = [word[0] for word in top_n_words]

    end = time.time()

    print("Total time ", end - start)

    # Write to out file
    with open(output_file, "w") as file:
        json.dump(out_dict, file)


if __name__ == "__main__":
    main(sys.argv[1:])

