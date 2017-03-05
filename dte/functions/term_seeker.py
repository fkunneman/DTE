
from collections import defaultdict

class TermSeeker:

    def __init__(self):
        self.tweets = []
        self.term_counts = defaultdict(int)
        self.term_tweets = defaultdict(list)

    def set_tweets(self,tweets):
        self.tweets = tweets

    def query_terms(self,terms,tweets=False):
        qs = set(terms)
        for tweet in tweets:
            if qs & set(tweet.entities)
                ste = set(tweet.entities)
                for term in terms:
                    if set([term]) & ste:
                        self.term_counts[term] += 1
                        if tweets:
                            self.term_tweets[term].append(tweet)

