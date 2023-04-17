from multiprocessing import Manager
import multiprocessing
import re
import sys
import os

# This variable is to remove noise
expresion = re.compile(r'\,|\.|\;|\:|\-|\'')


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


def reduce(key, val):
    # number of values with the same key
    count = sum(val)
    return (key, count)


if __name__ == "__main__":
    file = "ArcTecSw_2023_BigData_Practica_Part1_Sample"
    f = open(file, "r", encoding="utf_8")

    lines = f.readlines()

    maped = []
    for line in lines:
        maped.append(map(line))

    shuffled = shuffle(maped)

    result = []
    for key, val in shuffled.items():
        result.append(reduce(key, val))

    pass
