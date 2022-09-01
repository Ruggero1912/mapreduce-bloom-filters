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

def getHash (movie_id, m, i):
    #tmp = [f'{string.hexdigits[(char >> 4) & 0xf]}{string.hexdigits[char & 0xf]}' for char in mmh3.hash_bytes(movie_id, i)]
    #return abs(int(''.join(tmp), base=16)) % m

    #tmp = [((char >> 4) & 0xf) | ((char << 4) & 0xf0) for char in mmh3.hash_bytes(movie_id, i)]
    #return int.from_bytes(tmp, '')
    return abs(mmh3.hash(movie_id, i)) % m

def initializeBloomFilter(movie_id, m, k) -> bitarray:
    bs = bitarray(m)
    bs.setall(0)
    for i in range(k):
        bs[ getHash(movie_id, m, i) ] = True
    return bs

def addToFilter(bloom_filter, movie_id, m, k) -> bitarray:
    for i in range(k):
        bloom_filter[ getHash(movie_id, m, i) ] = True
    return bloom_filter

def checkInFilter(bloom_filter, movie_id, m, k) -> bool:
    assert type(movie_id) == str, f"the type of the given movieid is {type(movie_id)} content: {movie_id}"
    if(not bloom_filter or bloom_filter is None):
        return False
    #assert isinstance(bloom_filter, bitarray) , f"the type of bloom filter is not array"
    #if( bloom_filter.__len__() == 0):
    #    return False
    for i in range(k):
        if bloom_filter[ getHash(movie_id, m, i) ] == False:
            return False
    return True


def job1(ratings : RDD) -> dict:
    """
    - returns the number of films for each rating level
    """
    return ratings.countByKey()

def job2_base(ratings : RDD, M : list, K : list) -> list:
    """
    classic map reduce approach which introduces a lot of overhead for the transmission of the bloom filters between each step
    """
    def map(in_key : int , movie_id : str) -> tuple:
        key = int(key)
        assert( key >= 1 and key <= 10) , f"the input key is not in range(1,10) | input value: {in_key}"
        return ( key, initializeBloomFilter(movie_id, M[key - 1], K[key - 1]) )

    def reduce(bf1 : bitarray, bf2 : bitarray) -> bitarray:
        return bf1.__or__(bf2)

    ratings.map(map).reduceByKey(reduce).collect()


def job2_groupByKey(ratings : RDD, M : list, K : list) -> list:
    """
    alternative implementation of the bloom filters generation in Spark for performance comparison
    it exploits RDD method groupByKey() that tends to be inefficient on keys with high numerosity, since
    every row for that is key processed by one single Spark executor
    - return list<tuple<roundedRating : int, bloom_filter : bitarray> bloom_filters
    - inside ratings it expects to find a RDD in which each tuple is in the form <rounded_rating : int, movie_id : str>
    """ 
    def fun(tuple : tuple) -> tuple:
        """
        the input tuple is expected to be in the form <key : int, list_of_movies : Iterable(str) > 
        """
        in_key, list_of_movies = tuple
        key = int(in_key)
        assert (key >= 1 and key <= 10 ) , f"the input key is not in range(1,10) | input value: {in_key}"
        bs = bitarray(M[key - 1])
        bs.setall(0)
        for movie_id in list_of_movies:
            bs = addToFilter(bs, movie_id, M[key - 1], K[key - 1])
        return (key, bs)
    return ratings.groupByKey().map(fun).collect()

def job2_aggregateByKey(ratingsKKV: RDD, M : list, K : list) -> list:
    """
    creates and sets the bits of a bloom filter for each rating level
    - input list M: the number of bits for each bloom filter
    - input list K: the number of hash function to apply to each tested value
    - returns a bloom filter for each rating level
    """
    #now each rating is in the form (roundedRating, filmID)    i.e. : (6, 'tt0000001')
    
    def seqFunc(bs : bitarray, row2 : tuple) -> bitarray:
        
        key = row2[0]

        if( not bs ): # zeroValue case , here it is not set the length of the bitset
            return initializeBloomFilter(row2[1], M[key - 1], K[key - 1])
        return addToFilter(bs, row2[1], M[key - 1], K[key - 1])

    def combFunc(bitset1 : bitarray, bitset2 : bitarray) -> bitarray:
        assert bitset1.__len__() == bitset2.__len__() , f"different bitsets length! bitset1 len: { bitset1.__len__()} bitset2 len: {bitset2.__len__()}"
        if( not bitset1):
            print(f"strange case happened")
            return bitset2
        return bitset1.__or__(bitset2)

    
    # (roundedRating, (roundedRating, filmID ) )    i.e. : (6, (6, 'tt0000001') )
    bitsets = ratingsKKV.aggregateByKey(zeroValue=bitarray(), seqFunc=seqFunc, combFunc=combFunc)

    return bitsets.collect()
    
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

def job3(ratingsKKV : RDD, bitsets : list, M : list, K : list) -> list:
    """
    bitsets is a list, at the index (rating - 1) we have the bloom filter for the rating rating

    < FP - FN - TP - TN >
    """
    FP_OFFSET = 0
    FN_OFFSET = 1
    TP_OFFSET = 2
    TN_OFFSET = 3
    # (key, (key, value))
    def seqOp(scores : list, row) -> list:
        for index, bloom_filter in enumerate(bitsets):
            if checkInFilter(bloom_filter, row[1][1], M[index], K[index]):
                if index == (row[1][0] - 1):
                    scores[ ( index ) * 4 + TP_OFFSET] += 1
                else:
                    scores[ (index) * 4 + FP_OFFSET] += 1
            else:
                if index == (row[1][0] - 1):
                    scores[ ( index ) * 4 + FN_OFFSET] += 1
                else:
                    scores[( index ) * 4 + TN_OFFSET] += 1
        return scores
    """
    def combOp(scores1 : list, scores2 : list) -> list:
        assert len(scores1) == len(scores2)
        for i in range(len(scores1)):
            scores1[i] += scores2[i]
        return scores1
    """
    def combOp(scores1: list, scores2: list) -> list:

        assert len(scores1) == len(scores2) == 40 , f"LANDRY len: {len(scores1)} | content scores1: {scores1}"
        return [sum(x) for x in zip(scores1, scores2)]

    return ratingsKKV.aggregate(zeroValue=( [0] * 40), seqOp=seqOp, combOp=combOp)

    """
    zeroValue = [
            fp1, ... fp10, 
            fn1, ... fn10,
            tp1, ... tp10,
            tn1, ... tn10
        ]
    zeroValue = [
        < FP1 - FN1 - TP1 - TN1 >,
        < FP2 - FN2 - TP2 - TN2 >,
        ...
        < FP10 - FN10 - TP10 - TN10 >,
    ]
    rating = 3
    caso FP:
    FP_displacement = 0
        zv = [ ( rating - 1 ) * 4 + FP_displacement ]

    """
    pass

JOB_2_BASE = "base"
JOB_2_GROUP_BY_KEY = "group_by_key"
JOB_2_AGGREGATE_BY_KEY = "aggregate_by_key"

JOB_2_DEFAULT = JOB_2_AGGREGATE_BY_KEY

JOB_2_TYPES = {
    JOB_2_BASE              : job2_base, 
    JOB_2_AGGREGATE_BY_KEY  : job2_aggregateByKey, 
    JOB_2_GROUP_BY_KEY      : job2_groupByKey
    }

import types


def main(input_file_path="data.tsv", verbose=False, job2_type=JOB_2_DEFAULT, wait_to_close=0):

    if job2_type not in JOB_2_TYPES.keys():
        print(f"the specified job2 type '{job2_type}' was not recognised, allowed options: ", JOB_2_TYPES.keys(), f"\nGoing to use default: {JOB_2_DEFAULT}")
        job2_type = JOB_2_DEFAULT

    job2 = JOB_2_TYPES[job2_type]

    assert isinstance(job2, types.FunctionType)

    master_type = "yarn"   # "local" "yarn"
    conf = SparkConf().setMaster(master_type).setAppName(f"MRBF-deploy {master_type}-job2type {job2_type}-verbose {verbose}")
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
    ratingsKKV = ratings.map(lambda x: (x[0], (x[0], x[1])))
    print(f"Going to start job2 execution... Specified kind: ", job2_type)
    if job2_type == JOB_2_AGGREGATE_BY_KEY:
        tmp = job2(ratingsKKV, M, K)
    else:
        tmp = job2(ratings, M, K)

    end_time = time.time()
    print(f"job2 finished - elapsed time: {round(end_time - start_time, 3)} seconds")
    if verbose:
        print(f"\nresults: \n{tmp}")
    bloom_filters = [None] * 10
    assert len(tmp) <= 10, "BIG ERROR"
    for key, bs in tmp:
        bloom_filters[key - 1] = bs
    if verbose:
        print(f"\nreordered: {bloom_filters}")

    start_time = time.time()
    results = job3(ratingsKKV, bloom_filters, M, K)
    print(f"results type: {type(results)} | Content: {results}")
    scores_list = results
    end_time = time.time()
    print(f"job3 finished - elapsed time: {round(end_time - start_time, 3)} seconds")
    print("rating | < FP - FN - TP - TN >")
    for index, el in enumerate(scores_list):
        if index % 4 == 0:
            print(f"\n{(index // 4) + 1} | ", end="")
        print(f" {el} ", end="")
    print()

    if(wait_to_close > 0):
        print(f"going to sleep for {wait_to_close} seconds before closing Spark Context")
        from time import sleep
        sleep(wait_to_close)

    print("Shutting down Spark... ")
    sc.stop()


import sys, os

if __name__ == "__main__":
    
    print("usage: main.py <input file path> <opt:job_2_type:str> <opt:verbose:bool> <opt:wait x seconds before closing:int>")

    os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"

    input_file_name = "data.tsv"
    if sys.argv[1]:
        input_file_name = sys.argv[1]
    
    verbose = False
    wait_to_close = 0
    job2_type = JOB_2_DEFAULT

    if len(sys.argv) > 2:
        job2_type = sys.argv[2]
        print(f"specified job2 type : {job2_type}")
        if job2_type not in JOB_2_TYPES:
            print(f"the specified job2 type '{job2_type}' was not recognised")
            print("allowed options: ", JOB_2_TYPES.keys())
            exit(0)

    if len(sys.argv) > 3:
        verbose = not ( str.lower(sys.argv[3]) in ["0", "false", ""] )

    if len(sys.argv) > 4 and sys.argv[4]:
        wait_to_close = int(sys.argv[4])

    main(input_file_path=input_file_name, verbose=verbose,
         job2_type=job2_type, wait_to_close=wait_to_close)
