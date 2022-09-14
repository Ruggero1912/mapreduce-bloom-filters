from pyspark import SparkConf, SparkContext, RDD
import time
import math
from bitarray import bitarray
import mmh3
import pandas as pd

#MASTER_NODE_IP = "172.16.4.188"
#TOTAL_NUM_EXECUTORS = 12
#TOTAL_CORES_PER_EXECUTOR = 4

class Utils:

    TOTAL_NUM_EXECUTORS = 4
    TOTAL_CORES_PER_EXECUTOR = 4

    def set_num_executors(how_many : int):
        if how_many <= 0 :
            print(f"ERROR: the specified num executor {how_many} is not valid! keeping {Utils.TOTAL_NUM_EXECUTORS}")
            return
        #global TOTAL_NUM_EXECUTORS
        Utils.TOTAL_NUM_EXECUTORS = how_many

    def set_cores_per_executor(how_many : int):
        if how_many <= 0:
            print(f"ERROR: the specified num executor {how_many} is not valid! keeping {Utils.TOTAL_CORES_PER_EXECUTOR}")
            return
        if how_many > 4:
            print(f"[!] WARNING: the executors of the cluster have 4 cores, cannot set more than 4! | specified {how_many}")
            return
        #global TOTAL_CORES_PER_EXECUTOR
        Utils.TOTAL_CORES_PER_EXECUTOR = how_many

    def get_num_partitions() -> int:
        return Utils.TOTAL_NUM_EXECUTORS * Utils.TOTAL_CORES_PER_EXECUTOR

CSV_FILE_NAME = "spark_results.csv"

BLACK_COLOR = "\033[90m"
RED_COLOR = "\033[91m"
GREEN_COLOR = "\033[92m"
YELLOW_COLOR = "\033[93m"
BLUE_COLOR = "\033[94m"
PURPLE_COLOR = "\033[95m"
CYAN_COLOR = "\033[96m"
WHITE_COLOR = "\033[97m"
DEFAULT_COLOR = WHITE_COLOR

def red(string):
    return RED_COLOR + string + DEFAULT_COLOR

def blue(string):
    return BLUE_COLOR + string + DEFAULT_COLOR

def green(string):
    return GREEN_COLOR + string + DEFAULT_COLOR


P_KIND = "fixed" # "ranges"
P_FIXED = 0.0001

#this dict must be sorted in ascending order
P_RANGES = {
    2  : P_FIXED,
    15 : P_FIXED,
    100: P_FIXED,
}

def getP(n, total_number) -> float:
    """
    returns the false positive rate according to the percentage of occurencies of the current score over the total
    """
    if P_KIND == "fixed":
        return P_FIXED
    perc = ( n / total_number ) * 100
    for range_limit, p_value in P_RANGES.items():
        if perc <= range_limit:
            return p_value
    #this should not happen, returns the last item of the dict
    return P_RANGES.get(P_RANGES.keys()[-1])
    #if(perc <= 2):
    #    return 0.00001
    #elif(perc <= 15):
    #    return 0.00001
    #else:
    #    return 0.00001

def getM(n, p) -> int:
    return int(-(n * math.log(p))/(math.log(2)**2))

def getK(m, n) -> int:
    return int((m/n) * math.log(2))

def get_hash_index(movie_id, m, i) -> int:
    #tmp = [f'{string.hexdigits[(char >> 4) & 0xf]}{string.hexdigits[char & 0xf]}' for char in mmh3.hash_bytes(movie_id, i)]
    #return abs(int(''.join(tmp), base=16)) % m

    #tmp = [((char >> 4) & 0xf) | ((char << 4) & 0xf0) for char in mmh3.hash_bytes(movie_id, i)]
    #return int.from_bytes(tmp, '')
    return abs(mmh3.hash(movie_id, i)) % m

def initializeBloomFilter(movie_id, m, k) -> bitarray:
    bs = bitarray(m)
    bs.setall(0)
    for i in range(k):
        bs[ get_hash_index(movie_id, m, i) ] = True
    return bs

def getIndexesTrueFor(movie_id : str, m, k) -> list:
    ret = []
    for i in range(k):
        ret.append( get_hash_index(movie_id, m, i) )
    return ret

def addToFilter(bloom_filter, movie_id, m, k) -> bitarray:
    for i in range(k):
        bloom_filter[ get_hash_index(movie_id, m, i) ] = True
    return bloom_filter

def checkInFilter(bloom_filter, movie_id, m, k) -> bool:
    assert type(movie_id) == str, f"the type of the given movieid is {type(movie_id)} content: {movie_id}"
    if(not bloom_filter or bloom_filter is None):
        return False
    #assert isinstance(bloom_filter, bitarray) , f"the type of bloom filter is not array"
    #if( bloom_filter.__len__() == 0):
    #    return False
    for i in range(k):
        if bloom_filter[ get_hash_index(movie_id, m, i) ] == False:
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
    def map(tup : tuple) -> tuple:
        (in_key, movie_id) = tup
        key = int(in_key)
        assert( key >= 1 and key <= 10) , f"the input key is not in range(1,10) | input value: {in_key}"
        return ( key, initializeBloomFilter(movie_id, M[key - 1], K[key - 1]) )

    def reduce(bf1 : bitarray, bf2 : bitarray) -> bitarray:
        return bf1.__or__(bf2)

    return ratings.map(map).reduceByKey(reduce).collect()

def job2_emit_indexes(ratings : RDD, M : list, K : list) -> list:
    """
    map reduce approach 
    introduces overhead for the transmission of the indexes set to true for each movie
    """
    def map(tup: tuple) -> tuple:
        (in_key, movie_id) = tup
        key = int(in_key)
        assert(key >= 1 and key <= 10), f"the input key is not in range(1,10) | input value: {in_key}"
        return (key, getIndexesTrueFor(movie_id, M[key - 1], K[key - 1]))

    def combine(indexes_list1 : list, indexes_list2 : list) -> list:
        return indexes_list1 + indexes_list2 # returns list with duplicates (potentially)
        #return list(set(indexes_list1 + indexes_list2)) # avoid duplicates! but seems too performance impacting

    def reduce(tup : tuple) -> bitarray:
        (in_key, list_of_indexes) = tup
        key = int(in_key)
        assert(key >= 1 and key <= 10), f"the input key is not in range(1,10) | input value: {in_key}"
        bloom_filter = bitarray(M[key - 1])
        bloom_filter.setall(0)
        for index in list_of_indexes:
            bloom_filter[index] = True
        return bloom_filter

    #return ratings.map(map).groupByKey().map(reduce).collect() <- not working - keeps failing
    return ratings.map(map).reduceByKey(combine).map(reduce).collect()



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

def job3(ratings : RDD, bitsets : list, M : list, K : list) -> list:
    """
    bitsets is a list, at the index (rating - 1) we have the bloom filter for the rating rating

    < FP - FN - TP - TN >
    """
    FP_OFFSET = 0
    FN_OFFSET = 1
    TP_OFFSET = 2
    TN_OFFSET = 3
    MULTI_POSITIVE_COUNTER_INDEX = 40
    EVALUATED_COUNTER_INDEX = 41
    # (key, (key, value))
    def seqOp(scores : list, row) -> list:
        positive_results_counter = 0
        valid = False
        key, movie_id = row
        for index, bloom_filter in enumerate(bitsets):
            if checkInFilter(bloom_filter, movie_id, M[index], K[index]):
                positive_results_counter += 1
                if index == (key - 1):    #case True Positive
                    scores[ ( index ) * 4 + TP_OFFSET] += 1
                    valid = True
                else:                           #case False Positive
                    scores[ (index) * 4 + FP_OFFSET] += 1
            else:
                if index == (key - 1):    #case False Negative
                    scores[ ( index ) * 4 + FN_OFFSET] += 1
                else:                           #case True Negative
                    scores[( index ) * 4 + TN_OFFSET] += 1
        assert valid is True, f"the current row {row} was not found inside its bloom filter!"
        if positive_results_counter > 1:
            #case in which more than one bloom filter returned positive result 
            scores[MULTI_POSITIVE_COUNTER_INDEX] += 1
        #else:
            #case in which only the right bloom filter returned True
            #pass
        scores[EVALUATED_COUNTER_INDEX] += 1
        return scores

    def combOp(scores1: list, scores2: list) -> list:

        assert len(scores1) == len(scores2) == 42 , f"[!] ERROR len: {len(scores1)} | content scores1: {scores1}"
        return [sum(x) for x in zip(scores1, scores2)]

    return ratings.aggregate(zeroValue=( [0] * 42), seqOp=seqOp, combOp=combOp)

    """
    scores = [
        < FP1 - FN1 - TP1 - TN1 >,
        < FP2 - FN2 - TP2 - TN2 >,
        ...
        < FP10 - FN10 - TP10 - TN10 >,
    index40:    multiple_positive_results,
    index41:    evaluated_counter
    ]
    rating = 3
    caso FP:
    FP_displacement = 0
        zv = [ ( rating - 1 ) * 4 + FP_displacement ]

    """
    pass

JOB_2_BASE = "base"
JOB_2_EMIT_INDEXES = "emit_indexes"
JOB_2_GROUP_BY_KEY = "group_by_key"
JOB_2_AGGREGATE_BY_KEY = "aggregate_by_key"

JOB_2_DEFAULT = JOB_2_AGGREGATE_BY_KEY

JOB_2_TYPES = {
    JOB_2_BASE              : job2_base, 
    JOB_2_EMIT_INDEXES      : job2_emit_indexes,
    JOB_2_AGGREGATE_BY_KEY  : job2_aggregateByKey, 
    JOB_2_GROUP_BY_KEY      : job2_groupByKey
    }

import types


def main(input_file_path="data.tsv", verbose=False, job2_type=JOB_2_DEFAULT, wait_to_close=0, STORE_RESULTS=False):

    if job2_type not in JOB_2_TYPES.keys():
        print(f"the specified job2 type '{job2_type}' was not recognised, allowed options: ", JOB_2_TYPES.keys(), f"\nGoing to use default: {JOB_2_DEFAULT}")
        job2_type = JOB_2_DEFAULT

    job2 = JOB_2_TYPES[job2_type]

    assert isinstance(job2, types.FunctionType)

    master_type = "yarn"   # "local" "yarn"
    conf = SparkConf().setMaster(master_type)\
            .setAppName(f"MRBF|deploy-{master_type}-|job2type-{job2_type}-|verbose-{verbose}-|P-{P_RANGES}")\
            .set("spark.executor.instances",    str(Utils.TOTAL_NUM_EXECUTORS)        )\
            .set("spark.executor.cores",        str(Utils.TOTAL_CORES_PER_EXECUTOR)   )  # number of cores on each executor
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input_file_path)    #automatically splits the file on '\n'
    #now each row is in the form `tt0000001       5.7     1905`
    def row_parser(row : str) -> tuple:
        splitted = row.split('\t')
        return (round(float( splitted[1] ) + 0.0001), splitted[0])
    ratings = lines.map(row_parser)
    #ratings = lines.map(lambda row : (round( row.split('\t')[1] ), row.split('\t')[0]))
    print(blue(f"\n\n\tthe input dataset was originally split in {ratings.getNumPartitions()} \n\n"))

    ratings = ratings.repartition( Utils.get_num_partitions() )

    print(blue(f"\n\n\tnow the input dataset is split in {ratings.getNumPartitions()} \n\n"))
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
    job1_time_seconds = round(end_time - start_time, 3)
    print(green(f"job1 finished - elapsed time: {job1_time_seconds} seconds - total number of rows: {total_number} | N for each rating level: {N.items()}"))

    print(f"calculated values for \nP: {P}\n\nM: {M}\n\nK: {K}\n\n")
    start_time = time.time()
    ratingsKKV = ratings.map(lambda x: (x[0], (x[0], x[1])))

    print(blue(f"\n\n\tnow the ratingsKKV RDD is split in {ratingsKKV.getNumPartitions()} \n\n"))

    print(f"Going to start job2 execution... Specified kind: ", job2_type)
    if job2_type == JOB_2_AGGREGATE_BY_KEY:
        tmp = job2(ratingsKKV, M, K)
    else:
        tmp = job2(ratings, M, K)

    end_time = time.time()
    job2_time_seconds = round(end_time - start_time, 3)
    print(green(f"job2 finished - elapsed time: {job2_time_seconds} seconds"))
    if verbose:
        print(f"\nresults: \n{tmp}")
    bloom_filters = [None] * 10
    assert len(tmp) <= 10, "BIG ERROR"
    for key, bs in tmp:
        bloom_filters[key - 1] = bs
    if verbose:
        print(f"\nreordered: {bloom_filters}")

    start_time = time.time()
    #results = job3(ratingsKKV, bloom_filters, M, K)
    results = job3(ratings, bloom_filters, M, K)
    print(f"results type: {type(results)} | Content: {results}")
    scores_list = results
    end_time = time.time()
    job3_time_seconds = round(end_time - start_time, 3)
    print(green(f"job3 finished - elapsed time: {job3_time_seconds} seconds"))
    print(blue("rating | < FP - FN - TP - TN >"))
    for index, el in enumerate(scores_list):
        if index % 4 == 0:
            print(f"\n{(index // 4) + 1} | ", end="")
        print(f" {el} ", end="")
        if index == 39:
            print()
            break
    print("------------------")
    MULTI_POSITIVE_COUNTER_INDEX = 40
    EVALUATED_COUNTER_INDEX = 41
    print(red(f"multiple positive results movies: \t\t{scores_list[MULTI_POSITIVE_COUNTER_INDEX]}" ))
    print(f"counter of evaluated movies: \t\t{scores_list[EVALUATED_COUNTER_INDEX]}" )
    print(blue(f"Multiple positive rate: {round( ( scores_list[MULTI_POSITIVE_COUNTER_INDEX] / scores_list[EVALUATED_COUNTER_INDEX] ) * 100, 5 )}%"))

    if(wait_to_close > 0):
        print(red(f"going to sleep for {wait_to_close} seconds before closing Spark Context"))
        from time import sleep
        sleep(wait_to_close)

    print(red("Shutting down Spark... "))
    sc.stop()

    print(green(f"P value: {P_FIXED} - P ranges: {P_RANGES} - P array: {P}"))
    print(blue(f"job1 time: {job1_time_seconds} seconds"))
    print(blue(f"job2 time: {job2_time_seconds} seconds"))
    print(blue(f"job3 time: {job3_time_seconds} seconds"))

    if STORE_RESULTS:
        try:
            df = pd.read_csv(CSV_FILE_NAME, index_col=0)
        except:
            df = pd.DataFrame()
        row = {
            "P_value" : P_FIXED,
            "job2 kind" : job2_type,
            "num_executors" : Utils.TOTAL_NUM_EXECUTORS,
            "num_cores_per_executor": Utils.TOTAL_CORES_PER_EXECUTOR,
            "job1_time" : job1_time_seconds,
            "job2_time" : job2_time_seconds,
            "job3_time" : job3_time_seconds,
            "multipositive_rate": scores_list[MULTI_POSITIVE_COUNTER_INDEX] / scores_list[EVALUATED_COUNTER_INDEX],
            "multipositive_counter": scores_list[MULTI_POSITIVE_COUNTER_INDEX],
            "RDD_partitions" : Utils.get_num_partitions()
        }
        df = df.append(row, ignore_index=True)
        #df.sort_values(by=keys, axis=1)
        df.to_csv(CSV_FILE_NAME)


def iterate_tests(input_file_name="data.tsv", specified_job2_type=None, EXECUTORS_NUMS_TESTING=False):
    """
    basic testing function used to iterate over Spark executions in order 
    to obtain performance results of different combinations of parameters
    """

    if specified_job2_type is None:
        JOB2_TYPES_ITER = JOB_2_TYPES
    else:
        JOB2_TYPES_ITER = [specified_job2_type]

    if EXECUTORS_NUMS_TESTING:
        print(blue(f"Going to iterate over different combinations of executor nums and cores per executors num..."))

    for p_value in [0.0000001]:   # , 0.01, 0.001, 0.0001, 0.00001, 0.000001, 0.000001
        global P_FIXED
        P_FIXED = p_value
        for job2_kind in JOB2_TYPES_ITER:
            if job2_kind == JOB_2_EMIT_INDEXES:
                #gives problems with low values of p
                continue
            if EXECUTORS_NUMS_TESTING:
                for num_executors in [16]:
                    Utils.set_num_executors(num_executors)
                    for num_cores in [1,2,3,4]:
                        Utils.set_cores_per_executor(num_cores)
                        print(blue(f"Going to start new iteration with P={P_FIXED} | job2kind={job2_kind} | TOTAL_NUM_EXECUTORS={Utils.TOTAL_NUM_EXECUTORS} | TOTAL_CORES_PER_EXECUTOR={Utils.TOTAL_CORES_PER_EXECUTOR}"))
                        main(input_file_name, False, job2_kind, 0, STORE_RESULTS=True)
            else:
                print(blue(
                    f"Going to start new iteration with P={P_FIXED} | job2kind={job2_kind} | TOTAL_NUM_EXECUTORS={Utils.TOTAL_NUM_EXECUTORS} | TOTAL_CORES_PER_EXECUTOR={Utils.TOTAL_CORES_PER_EXECUTOR} | how many partitions: {Utils.get_num_partitions()}"))
                main(input_file_name, False, job2_kind, 0, STORE_RESULTS=True)

import sys, os

if __name__ == "__main__":
    
    print("usage: main.py <input file path> <opt:job_2_type:str> <opt:verbose:bool> <opt:wait x seconds before closing:int> <opt:--iterative-tests>")


    input_file_name = "data.tsv"
    if len(sys.argv) > 1 and sys.argv[1]:
        input_file_name = sys.argv[1]
    else:
        print(red(f"Going to use default input file path : '{input_file_name}'"))
    
    verbose = False
    wait_to_close = 0
    job2_type = JOB_2_DEFAULT
    job2_type_specified = None

    if len(sys.argv) > 2:
        job2_type = sys.argv[2]
        print(f"specified job2 type : {job2_type}")
        if job2_type not in JOB_2_TYPES:
            print(f"the specified job2 type '{job2_type}' was not recognised")
            print("allowed options: ", JOB_2_TYPES.keys())
            exit(0)
        job2_type_specified = job2_type
    else:
        print(blue(f"Using default job 2 type: {job2_type}"))

    if len(sys.argv) > 3:
        verbose = not ( str.lower(sys.argv[3]) in ["0", "false", ""] )

    if verbose:
        print(blue("Going to run in verbose mode..."))

    if len(sys.argv) > 4 and sys.argv[4]:
        wait_to_close = int(sys.argv[4])
        print(red(f"I will wait for {wait_to_close} seconds before closing Spark context"))

    if "--iterative-tests" in sys.argv:
        #print(red(f"DEBUG: content of sys.argv: {sys.argv} \t\ttype: {type(sys.argv)}"))
        print(red("Going to iterate over tests... Ignoring the other parameters... This will take time"))
        iterate_tests(input_file_name, specified_job2_type=job2_type_specified)
        exit(0)

    main(input_file_path=input_file_name, verbose=verbose,
         job2_type=job2_type, wait_to_close=wait_to_close)
