# Streaming-Data
The Streaming data includes three algorithms: Bloom filtering, Flajolet-Martin algorithm, and reservoir sampling

### 1. Bloom Filter
Bloom Filtering algorithm is to estimate whether the city of a business in business_second.json has shown before in business_first.json.
I used the following hash function to hash the city name:

<img src="https://user-images.githubusercontent.com/25105806/115946357-79693900-a475-11eb-806c-ffdcc8a2df24.png" width="20%" height="20%">

where a, b is randomly picked large number, m is the length of bit array, which is just length of the bloom filter.
Note: the representation of bloom filter is a not a list 1 or 0, but instead a set of indices of all 1's. This way we can save some spaces.

### 2. Flajolet-Martin algorithm
Uses generate_stream.jar to simulate the data stream to work with. The implementation listens to port number 9999 locally.
FM algorithm estimate the number of unique cities within a window in the data stream by the following steps:

<img src="https://user-images.githubusercontent.com/25105806/115946523-9f430d80-a476-11eb-9770-1722c3a174e7.png" width="50%" height="50%">

Multiple hash functions are used to improve the estimation accuracy. The standard way to do this is shown below

<img src="https://user-images.githubusercontent.com/25105806/115946555-e0d3b880-a476-11eb-85ee-de058cac7109.png" width="40%" height="40%">

But for simplicity, instead of using kmeans to group multiple estimation result, I manually set a number of group to simulate the grouping process, turns out this solution works.

Results:
```
Time                  Ground Truth  Estimation
2021-04-23 14:45:40   24            32
2021-04-23 14:46:33   42            50
2021-04-23 14:48:46   62            59
```

### 3. Reservoir Sampling/Fixed Size Sampling
This implementation uses Twitter API of streaming to implement the fixed size sampling method (Reservoir Sampling Algorithm) and find popular tags with the top
3 frequencies on tweets based on the samples.
Assuming that the memory can only save 100 tweets, we need to use the fixed size sampling method to only keep part of the tweets as a sample in the streaming.

The idea of Reservoir Sampling is to keep a fixed size sample to work with, and either keep/replace existing element in the sample list with newly arrived element or discard the new element:

<img src="https://user-images.githubusercontent.com/25105806/115946731-07462380-a478-11eb-920e-752f8f526c0f.png" width="50%" height="50%">

I only take tweets with hashtags into account when doing Reservoir Sampling, that is, the hashtag will be counted. 
Newly arrived hashtag will be kept with the probability of 100/n, where n is the number of the newly arrived hashtag's index.

For example, the 101st element will be kept with a probability of 100/101, which is pretty high.

Sample data is obtained by Twitter's API. The program is set to listen to the tweets with following keywords: 
```
TOPIC_LIST = ['COVID19', 'SocialDistancing', 'StayAtHome', 'Trump', 'Quarantine', 'CoronaVirus', 'China', 'Wuhan', 'Pandemic']
```
Results:
```
...

The number of tweets with tags from the beginning: 59
COVID19 : 16
Covid19 : 6
China : 4

The number of tweets with tags from the beginning: 60
COVID19 : 17
Covid19 : 6
China : 3
India : 3
SOSIYC : 3
Unite2FightCorona : 3

...
```
