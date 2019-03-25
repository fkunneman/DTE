
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_filter
from dte.classes import event, tweet

#########################################
### All events
#########################################


@registercomponent
class CollectEventTweets(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()
    entity_burstiness = Parameter()

    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events'), InputFormat(self,format_id='entity_burstiness',extension='.burstiness.txt',inputparameter='entity_burstiness') ) ]

    def setup(self, workflow, input_feeds):

        tweet_collector = workflow.new_task('collect_event_tweets', CollectEventTweetsTask, autopass=False, burstiness_threshold=self.burstiness_threshold)
        tweet_collector.in_tweetdir = input_feeds['tweetdir']
        tweet_collector.in_events = input_feeds['events']
        tweet_collector.in_entity_burstiness = input_feeds['entity_burstiness']

        return tweet_collector

class CollectEventTweetsTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    in_entity_burstiness = InputSlot()

    def out_more_tweets(self):
        return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.more_tweets.events')

    def run(self):

        # read in burstiness
        print('Reading in bursty entities')
        with open(self.in_entity_burstiness().path,'r',encoding='utf-8') as file_in:
            bursty_entities = file_in.read().strip().split('\n')
        set_bursty_entities = set(bursty_entities)

        # read in tweets
        print('Reading tweets')
        tweetsubdirs = sorted([ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ])
        cursordate = '20070707' # initializing to print progress
        entity_tweetsequence = defaultdict(list)
        dates = []
        entities_date = []
        entity_tweets = defaultdict(list)
        for tweetsubdir in tweetsubdirs:
            subdirstr = tweetsubdir.split('/')[-1]
            print('SUBDIRSTR',subdirstr)
            new_outfile = self.out_more_tweets().path + '_' + subdirstr + '.json'
            # go through all tweet files
            tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
            for tweetfile in tweetfiles:
                print(tweetfile)
                datestr = tweetfile.split('/')[-1].split('.')[0].split('-')[0]
                if datestr != cursordate:
                    for entity in list(set_bursty_entities - set(entities_date)):
                        entity_tweetsequence[entity].append([])
                    for entity in list(set(entities_date)):
                        entity_tweetsequence[entity].append(entity_tweets[entity])
                    dates.append(cursordate)
                    cursordate = datestr
                    entities_date = []
                    entity_tweets = defaultdict(list)
                # read in tweets
                with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                    tweetdicts = json.loads(file_in.read())
                for td in tweetdicts:
                    if len(list(set_bursty_entities & set(list(td['entities'].keys())))) > 0:
                        tweetobj = tweet.Tweet()
                        tweetobj.import_tweetdict(td)
                    for term in list(set_bursty_entities & set(list(td['entities'].keys()))):
                        entity_tweets[term].append(tweetobj)
                        entities_date.append(term)
        for entity in list(set_bursty_entities - set(entities_date)):
            entity_tweetsequence[entity].append([])
        for entity in list(set(entities_date)):
            entity_tweetsequence[entity].append(entity_tweets[entity])
        dates.append(cursordate)

        # read in events
        term_events = defaultdict(list)
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
            for ed in eventdicts:
                if len(list(set_bursty_entities & set(list(ed['entities'].keys())))) > 0:
                    eventobj = event.Event()
                    eventobj.import_eventdict(ed)
                for term in list(set_bursty_entities & set(list(ed['entities'].keys()))):
                    term_events[term].append(eventobj)

        # for each entity
        print('Adding event tweets by entity')
        for entity in bursty_entities:
            print('Entity',entity.encode('utf-8'))
            # for each event
            if len(term_events[entity]) == 0:
                continue
            elif len(term_events[entity]) == 1:
                event = term_events[entity][0]
                event_date = ''.join(event['datetime'].split()[0].split('-'))
                event_date_index = dates.index(event_date)
                first = event_date_index - 100 if (event_date_index - 100) >= 0 else 0
                last = event_date_index + 100 if (event_date_index + 100) < len(dates) else len(dates)-1
                tweets = sum(entity_tweetsequence[entity][first:last],[])
                event.add_tweets(tweets)
            else:
                events = term_events[entity]
                event_dates = [''.join(event['datetime'].split()[0].split('-')) for event in events]
                event_date_indices = [dates.index(event_date) for event_date in event_dates]
                for i,event in enumerate(events):
                    index = event_date_indices[i]
                    if i == 0:
                        first = index - 100 if (index - 100) >= 0 else 0
                    else:
                        minus = 100 if index - event_date_indices[i-1] > 199 else ((index - event_date_indices[i-1]) / 2) 
                        first = index - minus
                    if i == len(events)-1:
                        last = index + 100 if (event_date_index + 100) < len(dates) else len(dates)-1
                    else:
                        plus = 100 if event_date_indices[i+1] - index > 199 else ((event_date_indices[i-1] - index) / 2) 
                        last = index + plus
                        tweets = sum(entity_tweetsequence[entity][first:last],[])
                        event.add_tweets(tweets)


#########################################
### New events
#########################################
