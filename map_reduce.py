import multiprocessing
from multiprocessing import Manager
import re
import sys
import os
import time

# This variable is to remove noise
expresion = re.compile(r'\,|\.|\;|\:|\-|\'')

# This variable is to establish the number of threads to use (default value is the number of threads available)
n_threads = multiprocessing.cpu_count()


def std_words(words):
    # words to lowercase
    words_low = [word.lower() for word in words]

    # Remove specified characters from words in the list
    characters = str.maketrans('', '', ',.;:-\'')

    return [word.translate(characters) for word in words_low]


def map(file_name, chunk_start, chunk_end):
    words = []
    pairs = {}
    text_chunk = []
    with open(file_name, 'r', encoding="utf-8") as f:
        # Moving stream position to `chunk_start`
        f.seek(chunk_start)

        # Read and process lines until `chunk_end`
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break
            words.append(std_words(line.split()))

    # Count words
    for line in words:
        for w in line:
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


def reduce(key, val, total, lock):
    # number of values with the same key
    count = sum(val)
    with lock:
        total.value += count
    return (key, count)


def write_result(file, result, total):
    # print every word found and its distribution
    print(file + ":\n")
    for key, val in result:
        print(key + " : " + str(round(((val/total) * 100), 2)) + "%")
    print("\n")
    
def calculate_chunks(file):
    chunk_args = []
    with open(file, 'r', encoding='latin1') as f:
            def is_start_of_line(position):
                if position == 0:
                    return True
                # check if the position is a start of line (previous position is end of file)
                f.seek(position - 1)
                return f.read(1) == '\n'

            def get_next_line_position(position):
                # Read the current line till the end
                f.seek(position)
                f.readline()
                # return a position after reading the line
                return f.tell()

            chunk_start = 0
            # iterate over all chunks to construct chunk_args list
            while chunk_start < file_size:
                chunk_end = min(file_size, chunk_start + chunk)

                # check that chunk ends at end of line
                while not is_start_of_line(chunk_end):
                    chunk_end -= 1

                # handle the case when a line is too long to fit the chunk size
                if chunk_start == chunk_end:
                    chunk_end = get_next_line_position(chunk_end)

                args = (file, chunk_start, chunk_end)
                chunk_args.append(args)

                # move to the next chunk
                chunk_start = chunk_end
    return chunk_args


if __name__ == "__main__":
    
    for file in sys.argv[1:]:
        # Variables needed for the map reduce process
        
        file = "data/" + file

        share_map_result = []
        share_reduced_result = []
        i = 1

        # create manager for global shared variable to accumulate total number of words
        manager = Manager()
        n_words = manager.Value(int, 0)
        lock = manager.Lock()

        # create one pool for mapping and another for reducing
        mapPool = multiprocessing.Pool(n_threads)
        reducePool = multiprocessing.Pool(n_threads)

        file_size = os.path.getsize(file)
        chunk = file_size // n_threads
        
        chunk_args = calculate_chunks(file)

        share_map_result = mapPool.starmap(map, chunk_args)

        # Wait for the map function
        mapPool.close()
        mapPool.join()

        # Shuffle the results
        shuffled = shuffle(share_map_result)

        # Reduce the results
        args = [(x[0], x[1], n_words, lock) for x in shuffled.items()]
        share_reduced_result = reducePool.starmap(reduce, args)

        # Wait for the reduce function
        reducePool.close()
        reducePool.join()

        # Write the result to a file
        write_result(file, share_reduced_result, n_words.value)
