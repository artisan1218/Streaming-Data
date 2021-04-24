from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json
import random
import sys
import datetime
from binascii import hexlify

HOST = "localhost"
PORT = 9999
BATCH_DURATION = 5 # input stream will send element every 5 seconds
WINDOW_LENGTH = 30 # each window contains 30 elements
SLIDING_INTERVAL = 10 # next window is 10 elements ahead of current window

output_file_path = r'output.csv'
num_group = 10
num_func_per_group = 5
m = 233333333
p = 5330786047 # a random large prime number

def getMedian(l):
    l = sorted(l)
    if len(l) % 2 == 0:
        return int((l[int(len(l)/2) - 1] + l[int(len(l)/2)]) / 2)
    else:
        return int(l[int(len(l)/2)])

def generateHashFunc(num_group, num_func_per_group):
    param_a_list = random.sample(range(1, sys.maxsize - 1), num_func_per_group*num_group)
    param_b_list = random.sample(range(2, sys.maxsize - 1), num_func_per_group*num_group)
    return param_a_list, param_b_list


def FM(stream_rdd):
    # stream_rdd is just a spark RDD object that contains all element in this window
    curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    city_stream = stream_rdd.distinct().collect()
    ground_truth = len(city_stream)

    estimation_list = list()
    for param_a, param_b in zip(a_list, b_list):
        longest_trailing_zeros = -1
        for city in city_stream:
            city_int = int(hexlify(city.encode('utf8')), 16)
            hashed = ((param_a * city_int + param_b) % m ) % p
            bin_hashed = bin(hashed)[2:]
            len_zero = len(str(bin_hashed).split('1')[-1])
            longest_trailing_zeros = max(longest_trailing_zeros, len_zero)
        estimation_list.append(2**longest_trailing_zeros)
    estimation_list = sorted(estimation_list)

    estimation_list = [estimation_list[i:i+num_func_per_group] for i in range(0, len(estimation_list), num_func_per_group)]
    estimation_list = [sum(group)/len(group) for group in estimation_list]
    window_estimate = int(getMedian(estimation_list))

    window_result = str(curr_time) + ',' + str(ground_truth) + ',' + str(window_estimate) + '\n'
    with open(output_file_path, 'a', encoding="utf-8") as output_file:
        output_file.write(window_result)

conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, BATCH_DURATION)

input_stream = ssc.socketTextStream(HOST, PORT) # connect the input stream to port, interval is 0.1s

with open(output_file_path, 'w', encoding="utf-8") as output_file:
    output_file.write("Time,Ground Truth,Estimation\n")

a_list, b_list = generateHashFunc(num_group, num_func_per_group)
stream_window = input_stream.window(WINDOW_LENGTH, SLIDING_INTERVAL)#Return a new DStream which is computed based on windowed batches of the source DStream.
stream = (stream_window.map(lambda rawLine:json.loads(rawLine)) #Return a new DStream by passing each element of the source DStream through a function func.
          .map(lambda jLine:jLine['city']) # keep only the city field
          .filter(lambda city:len(city)!=0) # filter out the empty city name
          .foreachRDD(FM)
          )

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
