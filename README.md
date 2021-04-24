# Streaming-Data
The Streaming data includes three algorithms: Bloom filtering, Flajolet-Martin algorithm, and reservoir sampling

### Bloom Filter
Uses generate_stream.jar to simulate the data stream to work with.
Bloom Filtering algorithm is to estimate whether the city of a business in business_second.json has shown before in business_first.json.
I used the following hash function to hash the city name:

![image](https://user-images.githubusercontent.com/25105806/115946357-79693900-a475-11eb-806c-ffdcc8a2df24.png)

where a, b is randomly picked large number, m is the length of bit array, which is just length of the bloom filter.
Note: the representation of bloom filter is a not a list 1 or 0, but instead a set of indices of all 1's. This way we can save some spaces.

