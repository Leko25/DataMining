import logging
import math
import os
import sys
import time
from collections import defaultdict
import itertools
from itertools import combinations

from pyspark import SparkConf, SparkContext

# Globals
SUPPORT = 0
BASKET_SIZE = 0
BUCKET_SIZE = 0

class Bitmap:
    def __init__(self, n):
        """
        Assuming that ints are stored as 32 bits,
        then to store n bits we need n/32 + 1 integers
        @param n: number of bits
        """
        self.bit_vector = [0] * ((n >> 5) + 1)

    def get(self, pos):
        """
        Get the value of a bit at a given position
        @param pos: position of bit
        """

        self.idx = pos >> 5

        self.bit_num = pos & 0x1F

        # Find value of a given bit in bit_vector[index]
        return (self.bit_vector[self.idx] & (1 << self.bit_num)) != 0

    def setBit(self, pos):
        """
        Set the value of a bit at a given position
        @param pos: position of bit
        """

        # get bit vector position
        self.idx = pos >> 5

        self.bit_num = pos & 0x1F
        self.bit_vector[self.idx] |= (1 << self.bit_num)

def get_chunk_support(partition_size):
    return math.ceil(SUPPORT * partition_size/BASKET_SIZE)

def get_singletons(itemset, freq_itemset):
    freq_itemset_list = list(map(",".join, freq_itemset))
    return sorted(list(itemset.intersection(freq_itemset_list)))

def permutation_set(*args):
    return sorted(args)

def get_hash_sum(val):
    return sum(ord(char) for char in val)

def get_hash(args):
    hash_sum = 0
    for arg in args:
        hash_sum += get_hash_sum(arg)
    return hash(hash_sum) % BUCKET_SIZE

def first_pass(baskets):
    ps = get_chunk_support(len(baskets))

    singles_count = defaultdict(int)
    hash_table = defaultdict(int)
    freq_itemset = set()

    for items in baskets:
        for item in items:
            singles_count[item] += 1

        # Hash pairs
        for idx1 in range(len(items) - 1):
            for idx2 in range(idx1 + 1, len(items)):
                pairs = permutation_set(items[idx1], items[idx2])
                hash_val = get_hash(pairs)
                hash_table[hash_val] += 1

    # Get frequent itemset
    for k, v in singles_count.items():
        if v >= ps:
            freq_itemset.add(k)

    # Convert hash table to bitmap
    bitmap = Bitmap(BUCKET_SIZE)
    for k, v in hash_table.items():
        if v >= ps:
            bitmap.setBit(k)

    return freq_itemset, bitmap

def pcy(baskets):
    baskets = list(baskets)
    partition_size = len(baskets)

    ps = get_chunk_support(partition_size)
    
    freq_itemset, bitmap = first_pass(baskets)
    candidates_dict = defaultdict(list)
    idx = 1

    candidates_dict[idx] = [(item,) for item in freq_itemset]

    while True:
        idx += 1
        pairs_count = defaultdict(int)
        freq_pairs = []
        hash_table = defaultdict(int)

        for basket in baskets:
            itemsets = get_singletons(set(basket), set(candidates_dict[1]))
            if itemsets is None:
                continue

            pairs = combinations(itemsets, idx)
            next_pairs = combinations(itemsets, idx + 1)

            for pair in pairs:
                hash_val = get_hash(list(pair))
                if not bitmap.get(hash_val):
                    continue
                key = ' '.join(list(pair))
                pairs_count[key] += 1
                
            for next_pair in next_pairs:
                hash_val = get_hash(list(next_pair))
                hash_table[hash_val] += 1

        # Get frequent pairs
        for k, v in pairs_count.items():
            if v >= ps:
                freq_pairs.append(tuple(k.split()))

        # Break loop if no candidate pairs exist
        if len(freq_pairs) == 0:
            break

        # Add frequent pairs to candidate list
        candidates_dict[idx] = freq_pairs

        # Create new bit vector
        temp_bitmap = Bitmap(BUCKET_SIZE)
        for k, v in hash_table.items():
            if v >= ps:
                temp_bitmap.setBit(k)

        # Reset bit vector
        bitmap = temp_bitmap

    yield list(itertools.chain.from_iterable(candidates_dict.values()))

def make_string(list_items):
    result_str = ""
    idx = 1
    for list_item in list_items:
        stream = "("
        if len(list_item) == idx:
            for i in range(len(list_item)):
                if list_item[i] == list_item[-1]:
                    stream += "\'" + list_item[i] + "\'),"
                else:
                    stream += "\'" + list_item[i] + "\', "
            result_str += stream
        else:
            idx += 1
            result_str = result_str[:-1] + "\n\n"

            for i in range(len(list_item)):
                    if list_item[i] == list_item[-1]:
                        stream += "\'" + list_item[i] + "\'),"
                    else:
                        stream += "\'" + list_item[i] + "\', "
            result_str += stream
    return result_str

def file_writer(candidates, freq_itemsets, output_file):
    result_str = "Candidates:\n"

    candidate_str = make_string(candidates)
    result_str += candidate_str

    result_str = result_str[:-1] + "\n\n"

    result_str += "Frequent Itemsets:\n"
    
    freq_itemsets_str = make_string(freq_itemsets)
    result_str += freq_itemsets_str    
    
    result_str = result_str[:-1] + "\n"

    with open(output_file, "w") as file:
        file.write(result_str)

def son(itemset, candidates):
    items_count = defaultdict(int)
    for candidate in candidates:
        if set(candidate).issubset(set(itemset)):
            items_count[candidate] += 1
    return list(items_count.items())

def main(argv):
    global SUPPORT, BASKET_SIZE, BUCKET_SIZE

    # Unpack arguments
    threshold, s, input_file, output_file = argv

    SUPPORT = int(s)
    threshold = int(threshold)

    logging.getLogger("org").setLevel(logging.ERROR)

    config = SparkConf().set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    sc = SparkContext.getOrCreate(config)
    lines = sc.textFile(input_file, 3)

    rdd = lines.map(lambda x: (x.split(',')[0], x.split(',')[1])) \
                   .filter(lambda x: x[0] != "user_id") \
                   .groupByKey() \
                   .mapValues(lambda x: list(set(x))) \
                   .filter(lambda x: len(x[1]) >= threshold) \
                   .map(lambda x: x[1]).cache()

    BASKET_SIZE = rdd.count()
    BUCKET_SIZE = 10000000

    print("BASKET SIZE ------------------------> ", BASKET_SIZE)
    print("BUCKET SIZE ------------------------> ", BUCKET_SIZE)

    candidates = rdd.mapPartitions(lambda chunk: pcy(chunk)) \
                    .flatMap(lambda pairs: pairs) \
                    .distinct().sortBy(lambda pairs: (len(pairs), pairs)).collect()

    freq_itemsets = rdd.flatMap(lambda itemset: son(itemset, candidates)) \
                           .reduceByKey(lambda x, y: x + y, 8) \
                           .filter(lambda pairs: pairs[1] >= SUPPORT) \
                           .sortBy(lambda pairs: (len(pairs[0]), pairs[0])) \
                           .map(lambda pairs: pairs[0]) \
                           .collect()
        
    file_writer(candidates, freq_itemsets, output_file)

if __name__ == "__main__":
    start = time.time()
    main(sys.argv[1:])
    end = time.time()
    print("Duration: ", end - start)
