import os
from collections import defaultdict, OrderedDict
import time

data_dir = "dataset"

SUPPORT = 100

# Define collections
singles_count = defaultdict(int)
freq_itemset = set()

freq_pairs = set()
doubles_count = defaultdict(int)

hash_table = defaultdict(int)
baskets = []

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


def permutaion_set(*args):
    return sorted(args)

def get_baskets():
    global baskets

    with open(os.path.join(data_dir, "basket.txt"), "r") as file:
        while True:
            bakset = file.readline()

            if not bakset:
                break
            baskets.append(bakset)

def first_pass():
    global singles_count, freq_itemset, hash_table

    get_baskets()

    for basket in baskets:
        items = basket.split()
        for item in items:
            singles_count[item] += 1
        
        # Hash pairs
        for idx1 in range(len(items) - 1):
            for idx2 in range(idx1 + 1, len(items)):
                pairs = permutaion_set(items[idx1], items[idx2])
                hash_val = hash(pairs[0] + pairs[1]) % len(baskets)
                hash_table[hash_val] += 1
            
    # Get frequent itemset
    for k, v in singles_count.items():
        if v > SUPPORT:
            freq_itemset.add(k)

    # Convert hash_table to bitmap
    bitmap = Bitmap(len(baskets))
    for k, v in hash_table.items():
        # print(k, v)
        if v > SUPPORT:
            bitmap.setBit(k)

    # free up memory
    del singles_count, hash_table

    return bitmap

def second_pass(bitmap):
    global doubles_count
    for basket in baskets:
        items = basket.split()

        for idx1 in range(len(items) - 1):
            if items[idx1] not in freq_itemset:
                continue
            for idx2 in range(idx1 + 1, len(items)):
                if items[idx2] not in freq_itemset:
                    continue
                pairs = permutaion_set(items[idx1], items[idx2])
                hash_val = hash(pairs[0] + pairs[1]) % len(baskets)
                if not bitmap.get(hash_val):
                    continue
                doubles_count[pairs[0] + " " + pairs[1]] += 1

    for k, v in doubles_count.items():
        if v > SUPPORT:
            freq_pairs.add(k)
    
    for k in freq_pairs:
        print(k, doubles_count[k])

def main():
    bitmap = first_pass()
    second_pass(bitmap)

if __name__ == "__main__":
    main()