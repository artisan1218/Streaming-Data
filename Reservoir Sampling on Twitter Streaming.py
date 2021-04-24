import collections
import random
import sys
import tweepy

API_KEY = "I3PxQ76xUsD0mwuhPhPhL41lN"
API_SECRET = "HUF9iiKS0ubovOu7TbXhqgkFCED4enCiKYtwe7QOW7bAEX97gs"
ACCESS_TOKEN = "1384266740469682176-F0iSdDTwc9FKvdStCotCLbzbJTNx2t"
ACCESS_TOKEN_SECRET = "NYY5KHwMenLY2QPQJrEXc78zdp0to3Z44ALJvVuamXvy4"
TOPIC_LIST = ['COVID19', 'SocialDistancing', 'StayAtHome', 'Trump', 'Quarantine', 'CoronaVirus', 'China', 'Wuhan', 'Pandemic']


output_file_path = r'output.txt'

def outputResult(sample_list, seq, output_file_path):
    sample_count = collections.Counter(sample_list)
    sample_count = sorted(sample_count.items(), key=lambda pair:(-pair[1], pair[0]))

    outputFile = open(output_file_path, 'a')
    outputFile.write("The number of tweets with tags from the beginning: {}\n".format(seq))
    top3 = set() # keeps track of top3 frequencies
    for pair in sample_count:
        top3.add(pair[1])
        if len(top3) <= 3:
            outputFile.write(str(pair[0]) + ' : ' + str(pair[1]) + '\n')
    outputFile.write('\n')
    outputFile.close()

#override tweepy.StreamListener to add logic to on_status
class TwitterStreamListener(tweepy.StreamListener):
    
    def __init__(self, output_file_path):
        tweepy.StreamListener.__init__(self)
        self.sample_size = 100
        self.sample_list = list()
        self.sequence_num = 0
        self.output_file_path = output_file_path
        
    def on_status(self, status):
        hash_tag_list = status.entities['hashtags'] # get the hashtag of this tweet
        if len(hash_tag_list) > 0: # if there is hashtags within this tweet
            self.sequence_num += 1
            for hash_tag in hash_tag_list:
                hash_tag_text = hash_tag['text']
                if len(self.sample_list) < self.sample_size:
                    self.sample_list.append(hash_tag_text)
                else:
                    # keep or discard
                    keep_prob = float(100.0 / self.sequence_num)
                    if random.random() < keep_prob: #keep
                        # randomly choose one existing element and replace it with newly arrived element
                        self.sample_list[random.randint(0, self.sample_size-1)] = hash_tag_text
            #output result
            outputResult(self.sample_list, self.sequence_num, self.output_file_path)
        else:
            pass

auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

streamListener = TwitterStreamListener(output_file_path)
stream = tweepy.Stream(auth = auth, listener=streamListener)

stream.filter(track=TOPIC_LIST, languages=["en"])

