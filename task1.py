from pyspark import SparkContext, SparkConf
import json
import random
import sys
import time
from binascii import hexlify

startTime = time.time()

conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

input_file_path_first = sys.argv[1] #r'business_first.json'
input_file_path_second = sys.argv[2] # r'business_second.json'
output_file_path = sys.argv[3] #r'task1_output.csv'

city_int = (sc.textFile(input_file_path_first) # read file
               .map(lambda line:json.loads(line)) # convert each line to json object
               .map(lambda jLine:jLine['city']) # keep only the city field
               .filter(lambda city:len(city)!=0) # filter out the empty city
               .map(lambda cityStr:int(hexlify(cityStr.encode('utf8')), 16)) # convert city string to an integer
               .distinct() # remove duplicate city
               .collect()
            )

def makePrediction(city):
    if len(city) == 0:
        return 0
    else:
        cityStr = int(hexlify(city.encode('utf8')), 16)
        return predict(cityStr)

def predict(cityInt):
    if all(calculateHashedValue(a, b, len_bit_array, cityInt) in bloom_filter for a, b in zip(a_list, b_list)):
        return 1
    else:
        return 0

def calculateHashedValue(a, b, m, x):
    return (a * x + b) % m

def generateHashFunc(num_func):
    param_a_list = random.sample(range(1, sys.maxsize - 1), num_func)
    param_b_list = random.sample(range(0, sys.maxsize - 1), num_func)
    
    return param_a_list, param_b_list

def buildBloomFilter(city_int_list, len_bit_array, param_a_list, param_b_list):
    # bloom filter is not actually a list full of 0's and 1's, instead is represented as a set of indices
    # if there is an 1 in pos i, then add i to the set. If a pos is not in the set, then it's a 0
    bloom_filter = set() 
    
    param_m = len_bit_array  
    for city_int in city_int_list:
        for param_a, param_b in zip(param_a_list, param_b_list):
            hashed = calculateHashedValue(param_a, param_b, param_m, city_int)
            bloom_filter.add(hashed)
    return bloom_filter
   
# build the bloom filter using list of stream element
num_func = 2
len_bit_array = 10000
a_list, b_list = generateHashFunc(num_func)
bloom_filter = buildBloomFilter(city_int, len_bit_array, a_list, b_list)

prediction = (sc.textFile(input_file_path_second) # read file
                .map(lambda line:json.loads(line)) # convert each line to json object
                .map(lambda jLine:jLine['city']) # keep only the city field
                .map(lambda cityStr:makePrediction(cityStr)) 
                .collect()
             )

output_file = open(output_file_path, 'w')
output_file.write(' '.join([str(cityInt) for cityInt in prediction]))
output_file.close()

print('Duration:', str(time.time()-startTime))



