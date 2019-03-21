
from collections import defaultdict

class TermSeeker:

    def __init__(self):
        self.tweets = []
        self.term_counts = defaultdict(int)
        self.term_tweets = defaultdict(list)

    def set_tweets(self,tweets):
        self.tweets = tweets

    def query_terms(self,terms):
        qs = set(terms)
        for tweet in self.tweets:
            m = list(qs & set(tweet.entities))
            for term in m:
                self.term_counts[term] += 1
            r = qs - set(m)
            for term in r:
                self.term_counts[term] = 0 
#                    self.term_tweets[term].append(tweet)

