from collections import defaultdict, OrderedDict
import time
import os

## Info: Three pass apriori algorithm using "basket.txt"

data_dir = "dataset"

# Support threshold
SUPPORT=100

# Creates dictionary with default count as zero
singles_count = defaultdict(int)
freq_itemset = set()

doubles_count = defaultdict(int)
frequent_pairs = set()

triples_count = defaultdict(int)

# Standardize permuations to ensure that (a,b) == (b, a)
def permutaion_set(*args):
    return str(sorted(args))

# Generate all possible pairs
def generate_pairs(*args):
    pairs = []
    for idx1 in range(len(args) - 1):
        for idx2 in range(idx1 + 1, len(args)):
            pairs.append(permutaion_set(args[idx1], args[idx2]))
    return pairs

def first_pass():
    global singles_count, freq_itemset
    start = time.time()
    with open(os.path.join(data_dir, "basket.txt"), "r") as file:
        while True:
            line = file.readline()

            if not line:
                break
            
            items = line.split()

            for item in items:
                singles_count[item] += 1
    
    end = time.time()

    for k, v in singles_count.items():
        if v > SUPPORT:
            freq_itemset.add(k)

    print("First pass, total time: ", end - start)

    print("Number of frequent singles", len(freq_itemset))

    # print("Printing top 10 items")
    # singles_temp = sorted(singles_count.items(), key=lambda kv: kv[1], reverse=True)
    # sorted_dict = OrderedDict(singles_temp)
    # for k, v in sorted_dict.items():
    #     print(k, v)

def second_pass():
    global doubles_count, frequent_pairs
    start = time.time()
    with open(os.path.join(data_dir, "basket.txt"), "r") as file:
        
        while True:

            line = file.readline()

            if not line:
                break

            items = line.split()
            
            for idx1 in range(len(items) - 1):
                if items[idx1] not in freq_itemset:
                    continue
                for idx2 in range(idx1 + 1, len(items)):
                    if items[idx2] not in freq_itemset:
                        continue
                    pair = permutaion_set(items[idx1], items[idx2])
                    doubles_count[pair] += 1
            
    # get frequent pairs
    for k, v in doubles_count.items():
        if v > SUPPORT:
            frequent_pairs.add(k)

    end = time.time()
    print("Second pass, total time ", end - start)

def third_pass():
    start = time.time()
    global triples_count
    with open(os.path.join(data_dir, "basket.txt"), "r") as file:
        while True:

            line = file.readline()

            if not line:
                break

            items = line.split()

            for idx1 in range(len(items) - 2):
                if items[idx1] not in freq_itemset:
                    continue
                for idx2 in range(idx1 + 1, len(items) - 1):
                    if items[idx2] not in freq_itemset or permutaion_set(items[idx1], items[idx2]) not in frequent_pairs:
                        continue
                    
                    for idx3 in range(idx2 + 1, len(items)):
                        if items[idx3] not in freq_itemset:
                            continue
                        pairs = generate_pairs(items[idx1], items[idx2], items[idx3])
                        
                        if any(pair not in frequent_pairs for pair in pairs):
                            continue

                        triples = permutaion_set(items[idx1], items[idx2], items[idx3])
                        triples_count[triples] += 1
    end = time.time()
    print("Third pass, total time ", end - start)

    for k, v in triples_count.items():
        if v > SUPPORT:
            print(k + " :=  " + str(v))
            
def main():
    first_pass()
    second_pass()
    third_pass()

if __name__ == '__main__':
    main()