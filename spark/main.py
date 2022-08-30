from unicodedata import combining
from pyspark import SparkConf, SparkContext, RDD
import time
import math
from bitarray import bitarray
import mmh3

MASTER_NODE_IP = "172.16.4.188"

def getP(n, total_number) -> float:
    """
    returns the false positive rate according to the percentage of occurencies of the current score over the total
    """
    perc = ( n / total_number ) * 100
    if(perc <= 2):
        return 0.1
    elif(perc <= 15):
        return 0.01
    else:
        return 0.001

def getM(n, p) -> int:
    return int(-(n * math.log(p))/(math.log(2)**2))

def getK(m, n) -> int:
    return int((m/n) * math.log(2))

def initializeBloomFilter(movie_id, m, k) -> bitarray:
    bs = bitarray(m)
    for i in range(k):
        bs[ abs( mmh3.hash(movie_id, i) ) % m ] = True
    return bs

def addToFilter(bloom_filter, movie_id, m, k) -> bitarray:
    for i in range(k):
        bloom_filter[ abs( mmh3.hash(movie_id, i) ) % m ] = True
    return bloom_filter

def checkInFilter(bloom_filter, movie_id, m, k) -> bool:
    for i in range(k):
        if bloom_filter[ abs( mmh3.hash(movie_id, i) ) % m ] == False:
            return False
    return True


def job1(ratings : RDD) -> dict:
    """
    - returns the number of films for each rating level
    """
    return ratings.countByKey()

def job2(ratings: RDD, M : list, K : list) -> RDD:
    """
    creates and sets the bits of a bloom filter for each rating level
    - input list M: the number of bits for each bloom filter
    - input list K: the number of hash function to apply to each tested value
    - returns a bloom filter for each rating level
    """
    #now each rating is in the form (roundedRating, filmID)    i.e. : (6, 'tt0000001')
    
    def seqFunc(bs : bitarray, row2 : tuple) -> bitarray:
        
        key = row2[0]

        if( not bs): # zeroValue case , here it is not set the length of the bitset
            return initializeBloomFilter(row2[1], M[key - 1], K[key - 1])
        return addToFilter(bs, row2[1], M[key - 1], K[key - 1])

    def combFunc(bitset1 : bitarray, bitset2 : bitarray) -> bitarray:
        return bitset1.__or__(bitset2)

    kkvRDD = ratings.map(lambda x : (x[0], (x[0], x[1])))
    # (roundedRating, (roundedRating, filmID ) )    i.e. : (6, (6, 'tt0000001') )
    bitsets = kkvRDD.aggregateByKey(zeroValue=bitarray(), seqFunc=seqFunc, combFunc=combFunc)

    return bitsets
    
    """
    Aggregate the values of each key, using given combine functions and a neutral "zero value". This function can return a different result type, U, than the type of the values in this RDD, V. Thus, we need one operation for merging a V into a U and one operation for merging two U's, The former operation is used for merging values within a partition, and the latter is used for merging values between partitions. To avoid memory allocation, both of these functions are allowed to modify and return their first argument instead of creating a new U.

    V è una tupla del RDD iniziale
    U è una tupla in uscita da seqFunc

    zeroValue deve essere di tipo U

    seqFunc(U, V)

    combFunc(U, U)


    1 -> (49, gifusuhg)
    2 -> (45, 52sgdfgs)

    seqfunc( bitset vuoto,  1)          -> bitset_A

    seqFunc(bitset_A,       2)          -> bitset_B
    """

    """
    (6, 'tt0000001')        (6, 'tt0000001')
    (6, 'tt0000041')        (6, 'tt0000002')    ->  (6, ['tt0000001', 'tt0000002']) -> (6, bitset(00101))   OR  
                                                                                                               \
                                                                                                                --> (6, bitset(01101))
    (6, 'tt0000061')        (6, 'tt0000003')                                                                   /
    (6, 'tt0000023')    ->  (6, 'tt0000004')    ->  (6, ['tt0000003', 'tt0000004']) -> (6, bitset(01001))   OR    

    (6, 'tt0000124')        (5, 'tt0000005')
    (3, 'tt0000121')        (5, 'tt0000006')    ->  (5, ['tt0000005', 'tt0000006'])
    [...]

    (5, 'tt0000005')
    (5, 'tt0000006')
    """

    """
    (6, 'tt0000001')        (6, 'tt0000001')
    (6, 'tt0000041')        (6, 'tt0000002')    ->  COMBINE  ->  (6, [bitset(00001), bitset(00100)]) -> REDUCE -> (6, bitset(00101))   OR  
                                                                                                                                        \
                                                                                                                                         --> (6, bitset(01101))
    (6, 'tt0000061')        (6, 'tt0000003')                                                                                            /
    (6, 'tt0000023')    ->  (6, 'tt0000004')    ->  COMBINE  ->  (6, [bitset(00001), bitset(01000)]) -> REDUCE -> (6, bitset(01001))   OR    

    (6, 'tt0000124')        (5, 'tt0000005')
    (3, 'tt0000121')        (5, 'tt0000006')    ->  COMBINE  ->  (5, ['tt0000005', 'tt0000006'])
    [...]

    (5, 'tt0000005')
    (5, 'tt0000006')
    """


def main(input_file_path="data.tsv"):

    conf = SparkConf().setMaster("local").setAppName("Film Reviews bloom filters")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input_file_path)    #automatically splits the file on '\n'
    #now each row is in the form `tt0000001       5.7     1905`
    def row_parser(row : str) -> tuple:
        splitted = row.split('\t')
        return (round(float( splitted[1] ) + 0.0001), splitted[0])
    ratings = lines.map(row_parser)
    #ratings = lines.map(lambda row : (round( row.split('\t')[1] ), row.split('\t')[0]))

    #now each row is in the form (roundedRating, filmID)    i.e. : (6, 'tt0000001')
    start_time = time.time()
    N = job1(ratings)
    #assert type(N) == dict , N
    end_time = time.time()
    #inside N we have a dict in the form N = {1: n1, 2: n2, ..., 10: n10}
    total_number = 0
    M = [0] * 10
    K = [0] * 10
    P = [0] * 10
    
    for k, n in N.items():
        total_number += n

    for k, n in N.items():
        P[k - 1] = getP(n, total_number)
        M[k - 1] = getM(n, P[k - 1 ])
        K[k - 1] = getK(M[k - 1], n)

    print(f"job1 finished - elapsed time: {round(end_time - start_time, 3)} seconds - total number of rows: {total_number} | N for each rating level: {N.items()}")

    print(f"calculated values for \nP: {P}\n\nM: {M}\n\nK: {K}\n\n")
    start_time = time.time()
    bitsets = job2(ratings, M, K)
    tmp = bitsets.collect()
    end_time = time.time()
    print(f"job2 finished - elapsed time: {round(end_time - start_time, 3)} seconds")
    #- \n\nresults: \n{tmp}

import sys

if __name__ == "__main__":
    input_file_name = "data.tsv"
    if sys.argv[1]:
        input_file_name = sys.argv[1]
    main(input_file_path=input_file_name)