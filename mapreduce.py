import multiprocessing
from multiprocessing import Manager
import re
import sys
import os
import time

# This variable is to remove noise
expresion = re.compile(r'\,|\.|\;|\:|\-|\'')

# This variable is to establish the number of threads to use
#n_threads = multiprocessing.cpu_count()
n_threads = 2


def std_words(words):
    # words to lowercase
    words_low = [word.lower() for word in words]

    # Remove specified characters from words in the list
    characters = str.maketrans('', '', ',.;:-\'')

    return [word.translate(characters) for word in words_low]


def map(line):
    pairs = {}
    # Split line into words
    words = std_words(line.split())

    # Count words
    for w in words:
        if w in pairs:
            pairs[w] = pairs[w]+1
        else:
            pairs[w] = 1
    return pairs


def shuffle(pairs):
    res = {}
    for position in pairs:
        # Group every pair by key
        for key, val in position.items():
            if res.get(key) == None:
                res[key] = [val]
            else:
                res[key].append(val)
    return res


def reduce(args):
    # number of values with the same key
    key, val, total, lock = args
    count = sum(val)
    with lock:
        total.value += count
    return (key, count)


def write_result(file, result, total):
    # Write result to file
    with open(file + "_result.txt", "w") as f:
        f.write(file + "\n")
        for key, val in result:
            f.write(key + " : " + str(round(((val/total) * 100), 2)) + "%\n")


if __name__ == "__main__":
    
    print("Execution with n_threads: " + str(n_threads))
    
    for file in sys.argv[1:]:
        # Variables needed for the map reduce process

        share_map_result = []
        share_reduced_result = []
        i = 1

        manager = Manager()
        n_words = manager.Value(int, 0)
        lock = manager.Lock()

        # create one pool for mapping and another for reducing
        mapPool = multiprocessing.Pool(n_threads)
        reducePool = multiprocessing.Pool(n_threads)

        # Read the file
        #file = "ArcTecSw_2023_BigData_Practica_Part1_Sample"
        f = open(file, "r", encoding="utf_8")

        lines = f.readlines()

        start = time.time()

        share_map_result = mapPool.map(map, lines)

        # Wait for the map function
        mapPool.close()
        mapPool.join()

        # Shuffle the results
        shuffled = shuffle(share_map_result)

        # Reduce the results
        args = [(x[0], x[1], n_words, lock) for x in shuffled.items()]
        share_reduced_result = reducePool.map(reduce, args)

        # Wait for the reduce function
        reducePool.close()
        reducePool.join()

        end = time.time()

        print("Elapsed time: " + str(end - start) + " seconds")

        # Write the result to a file
        write_result(file, share_reduced_result, n_words.value)
