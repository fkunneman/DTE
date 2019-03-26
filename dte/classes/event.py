
import json
import re
import numpy
from collections import defaultdict
from collections import Counter

from dte.classes.tweet import Tweet
from dte.functions import time_functions

class Event:
    """
    Container for event class
    """
    def __init__(self):
        self.mongo_id = False
        self.datetime = False
        self.entities = []
        self.score = False
        self.location = False
        self.tweets = []
        self.tweets_added = []
        self.mentions = 1
        self.cycle = False
        self.periodicity = False
        self.predicted = False
        self.anticipointment = False
        self.eventtype = False
        self.status = False

    def import_eventdict(self,eventdict,txt=True):
        self.mongo_id = eventdict['mongo_id'] if 'mongo_id' in eventdict.keys() else False
        self.datetime = self.import_datetime(eventdict['datetime']) if 'datetime' in eventdict.keys() else False 
        self.entities = eventdict['entities'] if 'entities' in eventdict.keys() else False
        self.score = float(eventdict['score']) if 'score' in eventdict.keys() else False
        self.location = eventdict['location'] if 'location' in eventdict.keys() else False
        self.mentions = int(eventdict['mentions']) if 'mentions' in eventdict.keys() else False
        self.cycle = eventdict['cycle'] if 'cycle' in eventdict.keys() else False 
        self.periodicity = eventdict['periodicity'] if 'periodicity' in eventdict.keys() else False 
        self.predicted = eventdict['predicted'] if 'predicted' in eventdict.keys() else False
        self.anticipointment = float(eventdict['anticipointment']) if 'anticipointment' in eventdict.keys() else False
        self.eventtype = eventdict['eventtype'] if 'eventtype' in eventdict.keys() else False
        self.status = eventdict['status'] if 'status' in eventdict.keys() else 'stable'
        if txt:
            self.tweets = self.import_tweets(eventdict['tweets'],txt=True) if 'tweets' in eventdict.keys() else []
            self.tweets_added = self.import_tweets(eventdict['tweets_added'],txt=True) if 'tweets_added' in eventdict.keys() else []
        else:
            self.tweets = self.import_tweets(eventdict['tweets'],txt=False) if 'tweets' in eventdict.keys() else []
            self.tweets_added = self.import_tweets(eventdict['tweets_added'],txt=False) if 'tweets_added' in eventdict.keys() else []

    def return_dict(self,txt=True):
        eventdict = {
            'mongo_id':self.mongo_id,
            'datetime':str(self.datetime),
            'entities':self.entities,
            'score':self.score,
            'location':self.location,
            'cycle':self.cycle,
            'mentions':self.mentions,
            'periodicity':self.periodicity,
            'predicted':self.predicted,
            'anticipointment':self.anticipointment,
            'eventtype':self.eventtype,
            'status':self.status
        }
        if txt:
            eventdict['tweets'] = [tweet.return_dict(txt=True) for tweet in self.tweets]
            eventdict['tweets_added'] = [tweet.return_dict(txt=True) for tweet in self.tweets_added]
        else:
            eventdict['tweets'] = [tweet.return_dict(txt=False) for tweet in self.tweets]
            eventdict['tweets_added'] = [tweet.return_dict(txt=False) for tweet in self.tweets_added]
        return eventdict

    def import_datetime(self,datetime):
        date,time = datetime.split()
        dt = time_functions.return_datetime(date,time,minute=True,setting='vs')
        return dt

    def set_datetime(self,datetime):
        self.datetime = datetime

    def add_entities(self,entities):
        self.entities.extend(entities)

    def set_score(self,score):
        self.score = score

    def set_predicted(self):
        self.predicted = True

    def import_tweets(self,tweets,txt=True):
        imported_tweets = []
        for tweetdict in tweets:
            tweet = Tweet()
            tweet.import_tweetdict(tweetdict,txt=txt)
            imported_tweets.append(tweet)
        return imported_tweets

    def add_timex_tweet(self,tweet):
        if not set([tweet.id]) & set([x.id for x in self.tweets]) or set([tweet.id]) & set([x.id for x in self.tweets_added]):
            self.tweets.append(tweet)

    def add_tweet(self,tweet):
        if not set([tweet.id]) & set([x.id for x in self.tweets]) or set([tweet.id]) & set([x.id for x in self.tweets_added]):
            self.tweets_added.append(tweet)

    def add_tweets(self,tweets):
        for tweet in tweets:
            self.add_tweet(tweet)

    def add_mention(self,n=1):
        self.mentions += 1

    def set_cycle(self,cycle): # str: 'periodic' or 'aperiodic'
        self.cycle = cycle

    def set_periodicity(self,periodicity): # dict with periodic pattern, score and editions, if available
        self.periodicity = periodicity

    def set_status(self,status):
        self.status = status

    def merge(self,event):
        self.score = max(self.score,event.score)
        self.entities = list(set(self.entities + event.entities))
        tweetids = [tweet.id for tweet in self.tweets]
        self.tweets.extend([tweet for tweet in event.tweets if not set([tweet.id]) & set([x.id for x in self.tweets]) or set([tweet.id]) & set([x.id for x in self.tweets_added])])
        self.tweets_added.extend([tweet for tweet in event.tweets_added if not set([tweet.id]) & set([x.id for x in self.tweets]) or set([tweet.id]) & set([x.id for x in self.tweets_added])]) 
        self.mentions = len(self.tweets)
        self.resolve_overlap_entities()
        # self.order_entities()
        self.rank_tweets()
        if self.location == False and event.location != False:
            self.location = event.location
        else:
            self.set_event_location()
        if self.cycle == 'aperiodic' and event.cycle == 'periodic':
            self.cycle = 'periodic' 
            self.periodicity = event.periodicity
        if self.eventtype == False and event.eventtype != False:
            self.eventtype = event.eventtype
        if not (self.predicted and event.predicted): 
            self.predicted = False
        elif not (self.predicted) and event.predicted:
            self.predicted = event.predicted
        self.status = 'changed'            

    def entity_overlap(self,e1,e2):
        if set(e1.split()) & set(e2.split()):
            return True
        else:
            return False

    def merge_entities(self,e1,e2):
        if e1.strip('#') == e2.strip('#'): # only difference is hashtag
            if '#' in e1:
                return e1
            else:
                return e2
        elif not False in [part in e1.strip('#').split() for part in e2.strip('#').split()]: # e2 is n-gram subset of e1
            return e1
        elif not False in [part in e2.strip('#').split() for part in e1.strip('#').split()]: # e1 is n-gram subset of e2
            return e2

    def resolve_overlap_entities(self):
        non_overlapping_entities = [self.entities[0]]
        for entity2 in self.entities[1:]:
            overlap = False
            for entity1 in non_overlapping_entities:
                if self.entity_overlap(entity1,entity2):
                    entity1 = self.merge_entities(entity1.strip('#'),entity2.strip('#'))
                    overlap = True
                    break
            if not overlap:
                non_overlapping_entities.append(entity2)
        self.entities = non_overlapping_entities

    def order_entities(self):
        entity_ranks = defaultdict(list)
        # for every tweet
        for tweet in self.tweets:
            # rank the present entities by position (entity that is mentioned first is ranked 1)
            matching_entities = []
            for entity in self.entities:
                if re.search(re.escape(entity),tweet.text):
                    entity_position = re.search(re.escape(entity),tweet.text).span()[0]
                    matching_entities.append([entity,entity_position])
            sorted_matches = sorted(matching_entities, key = lambda k : k[1])
            for i,entity in enumerate(sorted_matches):
                entity_ranks[entity[0]].append(i)
        # order the entities by their mean ranks
        entities_avg_position_rank = [[entity,numpy.mean(entity_ranks[entity])] for entity in self.entities]
        sorted_entities = sorted(entities_avg_position_rank,key = lambda k : k[1])
        self.entities = [entity[0] for entity in sorted_entities]

    def rank_tweets(self):
        self.tweets = sorted(self.tweets,key = lambda k : k.datetime,reverse=True)

    def set_event_location(self,minimum_mentions=5,minimum_percentage=0.55):
        # count locations
        location_counter = Counter()
        for tweet in self.tweets:
            if len(tweet.cityrefs) > 0:
                for cityref in tweet.cityrefs:
                    location_counter[cityref] += 1
        # calculate percentages
        candidates = []
        for location in location_counter.keys():
            counts = location_counter[location]
            if counts >= minimum_mentions:
                candidates.append([location,counts/len(self.tweets)])
        # select location
        if len(candidates) > 0:
            top_candidate = sorted(candidates,key = lambda k : k[1],reverse=True)[0] 
            self.location = top_candidate[0] if top_candidate[1] > minimum_percentage else False 
        else:
            self.location = False
