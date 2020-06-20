import json
import math
import re
import string
import sys
import time
from collections import Counter

from pyspark import SparkConf, SparkContext

STOPWORDS = None

def loadStopWords(file_path):
    data = None
    with open(file_path, "rb") as file:
        data = file.read()
    return data.decode("utf-8").split()


def filter_words(text):
    words = text.translate(str.maketrans('','',string.punctuation))
    words = words.split()

    pattern = "[a-zA-Z]+"
    filtered_words = []
    for word in words:
        word = word.lower()
        if re.match(pattern, word) and word not in STOPWORDS and len(word) > 3:
            filtered_words.append(word)
    return filtered_words


def compute_tf(doc, business_id):
    word_dict = Counter(doc)
    n_count = len(doc)
    threshold = 3

    tf = []
    for word, count in word_dict.items():
        if count > threshold:
            tf.append(((business_id, word), count / float(n_count)))

    if len(tf) == 0:
        for word, count in word_dict.items():
            tf.append(((business_id, word), count / float(n_count)))
    return tf


def get_top_words(tfidf):
    tfidf = list(tfidf)
    tfidf.sort(key=lambda x: x[1])
    return [pair[0] for pair in tfidf[:200]]


def convert_to_model(profile, model_type):
    model = []
    if model_type.split("_")[1] == "profile":
        model = [{"description": model_type, "id": k, "profile": v} for k, v in profile.items()]
    else:
        model = [{"description": model_type, "id": k, "token": v} for k, v in profile.items()]
    return model


def main(argv):
    global STOPWORDS

    assert len(argv) == 3, "Script takes 3 arguments <train_file><model_file><stopwords>"

    train_file, model_file, stopwords_file  = argv

    config = SparkConf().setMaster("local[*]") \
            .setAppName("Task2") \
            .set("spark.executor.memory", "4g") \
            .set("spark.driver.memory", "4g")

    sc = SparkContext(conf=config).getOrCreate()

    STOPWORDS = loadStopWords(stopwords_file)

    lines = sc.textFile(train_file).map(json.loads).cache()

    user_dict = lines.map(lambda x: x["user_id"]).distinct().zipWithIndex().collectAsMap()

    business_dict = lines.map(lambda x: x["business_id"]).distinct().zipWithIndex().collectAsMap()

    model = convert_to_model(business_dict, "business_tokens")

    model += convert_to_model(user_dict, "user_tokens")

    business_text_tf = lines.map(lambda x: (business_dict[x["business_id"]], filter_words(x["text"]))) \
            .reduceByKey(lambda x, y: x + y, 7) \
            .flatMap(lambda x: compute_tf(x[1], x[0])) \
            .cache()

    num_doc = len(business_dict)

    business_text_idf = business_text_tf.map(lambda x: (x[0][1], x[0][0])) \
            .groupByKey() \
            .mapValues(lambda x: math.log(num_doc / len(set(x)))) \
            .collectAsMap()

    business_tfidf = business_text_tf.map(lambda x: (x[0][0], (x[0][1], x[1] * business_text_idf[x[0][1]]))) \
            .groupByKey() \
            .mapValues(get_top_words) \
            .cache()

    word_tokens = business_tfidf.flatMap(lambda x: x[1]).distinct().zipWithIndex().collectAsMap()

    business_profile = business_tfidf.mapValues(lambda x: [word_tokens[word] for word in x]).collectAsMap()

    user_profile = lines.map(lambda x: (user_dict[x["user_id"]], business_profile.get(business_dict[x["business_id"]]))) \
            .filter(lambda x: x[1] != None and len(x[1]) > 0) \
            .reduceByKey(lambda x, y: list(set(x)) + list(set(y))) \
            .collectAsMap()

    model += convert_to_model(business_profile, "business_profile")

    model += convert_to_model(user_profile, "user_profile")

    with open(model_file, "w+") as file:
        for line in model:
            file.writelines(json.dumps(line) + "\n")
        file.close()

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    print("Duration ", time.time() - start)
