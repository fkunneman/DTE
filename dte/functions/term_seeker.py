
from collections import Counter

class TermSeeker:

    def __init__(self):
        self.entity_counts = None
        self.occuring_entities = []
        
    def set_tweets(self,tweets_entities):
        self.entity_counts = Counter(tweets_entities,[]))
        self.occuring_entities = set(tweets_entities)

    def query_terms(self,terms):
        qs = set(terms)
        for term in list(self.occuring_entities & qs):
            self.tweet in self.occuring_entities:
        
        for tweet in self.tweets:
            m = list(qs & set(tweet.entities))
            occuring.extend(m)
            for term in m:
                self.term_counts[term] += 1
        remain = list(qs - set(occuring))
        for term in remain:
            self.term_counts[term] = 0 

