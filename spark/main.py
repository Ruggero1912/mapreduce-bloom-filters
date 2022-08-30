from pyspark import SparkConf, SparkContext, RDD
import time
import math
from bitarray import bitarray

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


def job1(ratings : RDD) -> dict:
    """
    - returns the number of films for each rating level
    """
    return ratings.countByKey()

def job2(ratings: RDD, M : list, K : list) -> dict:
    """
    creates and sets the bits of a bloom filter for each rating level
    - input list M: the number of bits for each bloom filter
    - input list K: the number of hash function to apply to each tested value
    - returns a bloom filter for each rating level
    """
    #now each rating is in the form (roundedRating, filmID)    i.e. : (6, 'tt0000001')
    def seqFunc(row1, row2):
        key = ??
        bitset1 = calcolaBitset(row1,m[key], k[key])
        bitset2 = calcolaBitset(row2)
        return bitset1 OR BIT A BIT bitset2
    def metodo(list):
        bitset
        for el in list:
            settabitset(el)
        pass
    #ratings.aggregateByKey(zeroValue=bitarray(), seqFunc=)

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
    pass

def main(input_file_path="data.tsv"):

    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input_file_path)    #automatically splits the file on '\n'
    #now each row is in the form `tt0000001       5.7     1905`
    def row_parser(row : str) -> tuple:
        splitted = row.split('\t')
        return (round(float( splitted[1] )), splitted[0])
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
        P[k] = getP(n, total_number)
        M[k] = getM(n, P[k])
        K[k] = getK(M[k], n)

    
        

    print(f"job1 finished - elapsed time: {round(end_time - start_time, 3)} seconds - total number of rows: {total_number} | N for each rating level: {N.items()}")


if __name__ == "__main__":
    main()