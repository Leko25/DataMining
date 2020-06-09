import os
from collections import defaultdict
from itertools import combinations

data_dir = "dataset"

SUPPORT = 10

# Collections
baskets = []
basket_size = 0

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


def get_basket():
    global basket_size, baskets
    with open(os.path.join(data_dir, "basket.txt"), "r") as file:
        while True:
            if not file.readline():
                break
            baskets.append(file.readline())
            basket_size += 1

def permutaion_set(*args):
    return sorted(args)

def get_hash(args):
    hash_string = ''.join(args)
    return hash(hash_string) % basket_size


def first_pass():
    get_basket()

    singles_count = defaultdict(int)
    hash_table = defaultdict(int)
    freq_itemset = set()

    for basket in baskets:
        items = basket.split()
        for item in items:
            singles_count[item] += 1

        # Hash pairs
        for idx1 in range(len(items) - 1):
            for idx2 in range(idx1 + 1, len(items)):
                pairs = permutaion_set(items[idx1], items[idx2])
                hash_val = get_hash(pairs)
                hash_table[hash_val] += 1

    # Get frequent itemset
    for k, v in singles_count.items():
        if v > SUPPORT:
            freq_itemset.add(k)

    # Convert hash_table to bitmap
    bitmap = Bitmap(basket_size)
    for k, v in hash_table.items():
        # print(k, v)
        if v > SUPPORT:
            bitmap.setBit(k)

    return freq_itemset, bitmap

def get_singletons(itemset, freq_itemset):
    return sorted(list(itemset.intersection(freq_itemset)))

def multi_candidate_pcy():
    freq_itemset, bitmap = first_pass()
    candidates_dict = defaultdict(list)
    idx = 1

    candidates_dict[idx] = [(item) for item in freq_itemset]

    while True:
        idx += 1
        pairs_count = defaultdict(int)
        freq_pairs = []
        hash_table = defaultdict(int)

        for basket in baskets:
            itemsets = get_singletons(set(basket.split()), set(candidates_dict[1]))
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
            if v > SUPPORT:
                freq_pairs.append(tuple(k.split()))

        # Break loop if no candidate pairs exist
        if len(freq_pairs) == 0:
            break

        # add frequent pairs to candidate list
        candidates_dict[idx] = freq_pairs

        # Create new bit vector
        temp_bitmap = Bitmap(basket_size)
        for k, v in hash_table.items():
            if v > SUPPORT:
                temp_bitmap.setBit(k)
        
        # Reset bit vector
        bitmap = temp_bitmap
    return candidates_dict
            
def main():
    result = multi_candidate_pcy()

    for k, v in result.items():
        print(k, v)

if __name__ == "__main__":
    main()