
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
        self.mentions = 1
        self.periodic = False
        self.anticipointment = False
        self.type = False
        self.status = 'unstable'
        self.predicted = False

    def import_eventdict(self,eventdict):
        self.mongo_id = eventdict['mongo_id'] if 'mongo_id' in eventdict.keys() else False
        self.datetime = self.import_datetime(eventdict['datetime']) if 'datetime' in eventdict.keys() else False 
        self.entities = eventdict['entities'] if 'entities' in eventdict.keys() else False
        self.score = float(eventdict['score']) if 'score' in eventdict.keys() else False
        self.location = eventdict['location'] if 'location' in eventdict.keys() else False
        self.tweets = self.import_tweets(eventdict['tweets']) if 'tweets' in eventdict.keys() else []
        self.mentions = int(eventdict['mentions']) if 'mentions' in eventdict.keys() else False
        self.periodic = eventdict['periodic'] if 'periodic' in eventdict.keys() else False 
        self.anticipointment = float(eventdict['anticipointment']) if 'anticipointment' in eventdict.keys() else False
        self.type = eventdict['type'] if 'type' in eventdict.keys() else False 
        self.status = eventdict['status'] if 'status' in eventdict.keys() else 'stable'
        self.predicted = eventdict['predicted'] if 'predicted' in eventdict.keys() else False

    def return_dict(self):
        eventdict = {
            'mongo_id':self.mongo_id,
            'datetime':str(self.datetime),
            'entities':self.entities,
            'score':self.score,
            'location':self.location,
            'tweets':[tweet.return_dict() for tweet in self.tweets],
            'mentions':self.mentions,
            'periodic':self.periodic,
            'anticipointment':self.anticipointment,
            'type':self.type,
            'status':self.status,
            'predicted':self.predicted
        }
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

    def set_status(self,status):
        self.status = status

    def predicted(self):
        self.predicted = True

    def import_tweets(self,tweets):
        imported_tweets = []
        for tweetdict in tweets:
            tweet = Tweet()
            tweet.import_tweetdict(tweetdict)
            imported_tweets.append(tweet)
        return imported_tweets

    def add_tweet(self,tweet):
        self.tweets.append(tweet)

    def add_mention(self,n=1):
        self.mentions += 1

    def set_periodic(self,periodic): # dictionary
        self.periodic = periodic

    def merge(self,event):
        self.score = max(self.score,event.score)
        self.entities = list(set(self.entities + event.entities))
        tweetids = [tweet.id for tweet in self.tweets]
        self.tweets.extend([tweet for tweet in event.tweets if not tweet.id in tweetids])
        self.mentions = len(self.tweets)
        self.status = 'unstable'
        if self.location == False and event.location != False:
            self.loation = event.location
        if self.periodic = False and event.periodic != False:
            self.periodic = event.periodic
        if self.type == False and event.type != False:
            self.type = event.type
        self.status = 'unstable'
        self.predicted = False

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
