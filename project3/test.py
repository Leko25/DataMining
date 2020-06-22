import os
import json
from pyspark import SparkContext

sc = SparkContext("local[*]", "Task2").getOrCreate()
data_dir = "/home/kelechi/Downloads/"

ground_truth = sc.textFile(os.path.join(data_dir, "test_review_ratings.json")) \
                 .map(json.loads).map(lambda x: (hash((x["user_id"], x["business_id"])), 0)).collectAsMap()
predictions = sc.textFile("predictions.json") \
                .map(json.loads) \
                .map(lambda x: (x["user_id"], x["business_id"])).collect()

count = 0
for k, v in ground_truth.items():
    if count == 10:
        break
    print(k, v)
    count += 1

print()
count = 0
for v in predictions:
    if count == 10:
        break
    print(hash(v))
    count += 1


for v in predictions:
    temp = hash(v)
    if temp in ground_truth.keys():
        ground_truth[temp] += 1

print ("Accuracy ", sum(ground_truth.values())/ len(ground_truth))
