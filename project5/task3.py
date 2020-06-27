import sys
import time
import random
import csv
import tweepy
from collections import defaultdict


# Globals
API_KEY = "U7HDa4CXTNlV1PgABgLQG3MF3"
SECRET_KEY = "N3OO7AXT6bpknBLVPwvCoNJuzWRkLPS8L2Y4tNyYaBrMA2ZuoW"
ACCESS_TOKEN = "1276329313638309888-NJzcNSx0qJklPNOmZaB6btdNz8eMb9"
ACCESS_TOKEN_SECRET = "nUn2WuJV6wLorQQ8JUInujIu13cfMnUoME34QTp2m3jl9"

WINDOW_LEN = 100


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, output_file):
        tweepy.StreamListener.__init__(self)
        self.output_file = output_file
        self.tags_freq = defaultdict(int)
        self.tweet_counter = 0
        self.current_tags_list = defaultdict(list)

        # create emppty csv file
        with open(self.output_file, "w") as file:
            pass

    def on_status(self, tweet_stream):
        tags = tweet_stream.entities.get("hashtags")  # List of dict items with keys -> "text", "indicies"

        if self.tweet_counter <= WINDOW_LEN:
            if len(tags) > 0:
                tag_list = []
                for tag_dict in tags:
                    tag = tag_dict.get("text")
                    tag_list.append(tag)

                    #self.tags_freq[tag] += 1

                    if self.tags_freq.get(tag):
                        self.tags_freq[tag] += 1
                    else:
                        self.tags_freq[tag] = 1

                self.current_tags_list[self.tweet_counter] = tag_list
                self.tweet_counter += 1

                self.print_results()
        else:
            if WINDOW_LEN/self.tweet_counter >= random.random():
                pos = random.choice(list(range(WINDOW_LEN)))
                if len(tags) > 0:
                    current_tag_list = self.current_tags_list[pos]
                    for current_tag in current_tag_list:

                        self.tags_freq[current_tag] -= 1

                        if self.tags_freq[current_tag] < 1:
                            del self.tags_freq[current_tag]

                    tag_list = []
                    for tag_dict in tags:
                        tag = tag_dict.get("text")
                        tag_list.append(tag)

                        if self.tags_freq.get(tag):
                            self.tags_freq[tag] += 1
                        else:
                            self.tags_freq[tag] = 1

                    self.current_tags_list[pos] = tag_list
                    self.tweet_counter += 1

                    self.print_results()

    def print_results(self):
        self.tags_freq = {k: v for k, v in sorted(self.tags_freq.items(), key=lambda kv: kv[0].lower())}
        self.tags_freq = {k: v for k, v in sorted(self.tags_freq.items(), key=lambda kv: kv[1], reverse=True)}

        print("The number of tweets with tags from the beginning: " + str(self.tweet_counter))
        for k, v in self.tags_freq.items():
            print(k + " : " + str(v))
        print()

    def write_file(self):
        with open(self.output, "a") as file:
            file.write("The number of tweets with tags from the beginning: " + str(self.tweet_counter) + "\n")

def main(argv):
    assert len(argv) == 2, "Script takes 2 arguments <port><output_file>"

    # Unpack arguments
    port, output_file = argv

    port = int(port)

    # Authentication
    auth = tweepy.OAuthHandler(API_KEY, SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth)

    stream_listener = MyStreamListener(output_file)

    stream = tweepy.Stream(auth = api.auth, listener=stream_listener)

    stream.filter(track=["BlackLivesMatter", "COVID19", "police", "NBA", "laliga"], languages=["en"])

if __name__ == "__main__":
    main(sys.argv[1:])
