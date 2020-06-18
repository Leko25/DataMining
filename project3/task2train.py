import os
import json
import time
import string
from collections  import Counter
from pyspark import SparkContext, SparkConf
import re
import math
import itertools
import sys

STOPWORDS = None

def loadStopWords(file_path):
    data = None
    with open(file_path, "rb") as file:
        data = file.read()
    return data.decode("utf-8").split()

def filter_words(text):
    words = text.translate(str.maketrans('', '', string.punctuation))
    words = words.split()

    pattern = "[a-zA-Z]+"
    filtered_words = []
    for word in words:
        word = word.lower()
        if re.match(pattern, word) and word not in STOPWORDS and len(word) > 1:
            filtered_words.append(word)
    return filtered_words


def compute_tf(doc):
    word_dict = Counter(doc)
    n_count = len(doc)
    tf = [(word, count / float(n_count)) for word, count in word_dict.items()]
    return tf

def compute_tfidf(tf_list, idf_dict):
    tfidf = {}
    for pair in tf_list:
        tfidf[pair[0]] = pair[1] * idf_dict[pair[0]]
    tfidf_sorted = [word for word, _ in sorted(tfidf.items(), key=lambda kv: kv[1], reverse=True)]
    return tfidf_sorted[:200]


def convert_to_model(profile, model_type):
    model = [{"model": model_type, "id": k, "profile": v} for k, v in profile.items()]
    return model

def main(argv):
    global STOPWORDS

    assert len(argv) == 3, "Script takes 3 arguments <train_file><model_file><stopwords>"

    # Unpack arguments
    train_file, model_file, stopwords_file = argv

    config = SparkConf().setMaster("local[*]") \
            .setAppName("Task2") \
            .set("spark.executor.memory", "4g") \
            .set("spark.driver.memory", "4g")

    sc = SparkContext(conf=config).getOrCreate().setLogLevel("ERROR")

    STOPWORDS = loadStopWords(stopwords_file)

    lines = sc.textFile(train_file)

    user_dict = lines.map(json.loads) \
            .map(lambda x: x["user_id"]) \
            .distinct() \
            .zipWithIndex() \
            .collectAsMap()

    business_dict  = lines.map(json.loads) \
            .map(lambda x: x["business_id"]) \
            .distinct().zipWithIndex() \
            .collectAsMap()

    rdd = lines.map(json.loads) \
            .map(lambda x: (user_dict[x["user_id"]], business_dict[x["business_id"]], x["text"])).cache()

    business_text = rdd.map(lambda x: (x[1], filter_words(x[2]))) \
            .reduceByKey(lambda x, y: x + y, 7) \
            .cache()

    business_text_tf = business_text.mapValues(compute_tf).cache()

    num_doc = len(business_dict)

    business_text_df = business_text.flatMap(lambda x: [(word, x[0]) for word in x[1]]) \
            .groupByKey(7, lambda x: hash(x) % 7) \
            .mapValues(lambda x: math.log(num_doc/len(set(x)))) \
            .collectAsMap()

    business_tfidf = business_text_tf.mapValues(lambda x: compute_tfidf(x, business_text_df))

    top_word_tokens = business_tfidf.flatMap(lambda x: x[1]) \
            .distinct() \
            .zipWithIndex() \
            .collectAsMap()

    business_profile = business_tfidf.mapValues(lambda x: [top_word_tokens[word] for word in x]).cache()

    # Create Business Profile
    business_profile_dict = business_profile.collectAsMap()

    user_profile = rdd.map(lambda x: (x[0], x[1])) \
            .mapValues(lambda x: business_profile_dict[x]) \
            .reduceByKey(lambda x, y: list(set(x)) + list(set(y))) \
            .filter(lambda x: len(x[1]) > 1)

    model = convert_to_model(business_profile_dict, "business_profile")

    # Create User Profile
    user_profile_dict = user_profile.collectAsMap()

    model = model.extend(convert_to_model(user_profile_dict, "user_profile"))

    with open(model_file, "w+") as file:
        for line in model:
            file.writelines(json.dumps(line) + "\n")
        file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
