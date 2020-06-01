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

def mapJsonObj(jsonObj):
    year = datetime.strptime(jsonObj["date"], "%Y-%m-%d %H:%M:%S").year
    return ((year, 1), (jsonObj["user_id"], 1), jsonObj["text"])

def loadStopWords(file_path):
    data = None
    with open(file_path, "rb") as file:
        data = file.read()
    return data.decode("utf-8").split()

def filterPunctuation(text):
    words = text.translate(str.maketrans('', '', string.punctuation))
    return words.split()

def filterWords(word):
    pattern = "[a-zA-Z]+"
    if re.match(pattern, word) and word not in stopwords:
        return True
    return False

def sort_values(results):
    temp_dict = {}

    for pair in results:
        if temp_dict.get(pair[1]) == None:
            temp_dict[pair[1]] = [pair[0]]
        else:
            temp_dict[pair[1]].append([pair[0]])

    sorted_val = []

    for k, values in temp_dict.items():
        values = sorted(values)

        for value in values:
            sorted_val.append([value, k])

    return sorted_val

def main(argv):
    global stopwords

    assert len(argv) == 6, "Script takes 6 arguments"

    input_file, output_file, stop_words_file, y_arg, m_arg, n_arg = argv

    if not isValidFiles(input_file, stop_words_file):
        print("Invalid file path argument")
        return

    y_arg = int(y_arg)
    m_arg = int(m_arg)
    n_arg = int(n_arg)

    start = time.time()

    out_dict = {}

    # Get Error Logs
    logging.getLogger("org").setLevel(logging.ERROR)

    conf = SparkConf().setMaster("local[*]").setAppName("Task1")
    sc = SparkContext(conf=conf).getOrCreate()

    lines = sc.textFile(input_file)
    rdd = lines.map(json.loads) \
               .map(lambda x: mapJsonObj(x)).cache()
    
    # Determine the reviews in a given year
    reviews_y = rdd.filter(lambda x: x[0][0] == y_arg) \
                   .map(lambda x: x[0]) \
                   .reduceByKey(lambda x, y: x + y)

    # Determine the top m users who have the larges number of reviews and their count
    user_reviews = rdd.map(lambda x: x[1]) \
                      .reduceByKey(lambda x, y: x + y) \
                      .sortBy(lambda x: x[1], ascending=False)

    total_reviews = rdd.count()
    unique_users = rdd.map(lambda x: x[1][0]).distinct().count()
    top_users = user_reviews.take(m_arg)

    stopwords = loadStopWords(stop_words_file)

    text_rdd = rdd.flatMap(lambda x: filterPunctuation(x[2])) \
                         .map(lambda x: (x.lower(), 1)) \
                         .filter(lambda x: filterWords(x[0])).cache()


    top_words = text_rdd.reduceByKey(lambda x, y: x + y, 8) \
                        .sortBy(lambda x: x[1], ascending=False)

    # Write output
    out_dict["A"] = total_reviews
    out_dict["B"] = reviews_y.collect()[0][1]
    out_dict["C"] = unique_users
    out_dict["D"] = sort_values(top_users)

    top_n_words = top_words.take(n_arg)
    top_n_words_list = [word[0] for word in top_n_words]

    out_dict["E"] = sorted(top_n_words_list)

    end = time.time()

    print("Taks 1", end - start)


    with open(output_file, "w") as file:
        json.dump(out_dict, file)

if __name__ == "__main__":
    main(sys.argv[1:])