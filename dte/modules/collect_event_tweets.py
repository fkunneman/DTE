
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_filter
from dte.classes import event, tweet

@registercomponent
class CollectEventTweets(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()
    entity_burstiness = Parameter()

    burstiness_threshold = Parameter()

    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events'), InputFormat(self,format_id='entity_burstiness',extension='.burstiness.json',inputparameter='entity_burstiness') ) ]

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

    burstiness_threshold = Parameter()

    def out_more_tweets(self):
        return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.more_tweets.events')

    def run(self):

        # read in burstiness
        print('Reading in burstiness numbers')
        burstiness_threshold = float(self.burstiness_threshold)
        bursty_entities = []
        with open(self.in_entity_burstiness().path,'r',encoding='utf-8') as file_in:
            for line in file_in.readlines():
                tokens = line.strip().split('\t')
                if float(tokens[3]) >= burstiness_threshold:
                    bursty_entities.append(tokens[0])
        bursty_entities = set(bursty_entities)

        # read in events
        date_term_events = defaultdict(lambda : defaultdict(list))
        events = []
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            entities = eventobj.entities
            be = list(set(entities) & bursty_entities) 
            if len(be) > 0: # event has bursty entities that can be used to collect more tweets
                # in a window of three weeks prior to the event and three weeks after the event 
                collect_start = eventobj.datetime.date() - datetime.timedelta(days=21)
                collect_end = eventobj.datetime.date() + datetime.timedelta(days=21)
                cursor = collect_start
                while cursor <= collect_end and cursor <= datetime.date.today():
                    for entity in be:
                        date_term_events[cursor][entity].append(eventobj)
                    cursor = cursor + datetime.timedelta(days=1)
            events.append(eventobj)

        # count current status
        all_tweets = sum([len(eventobj.tweets) for eventobj in events])
        print('STATUS AT START:',all_tweets,'TWEETS IN TOTAL')

        # go through all tweet dirs
        last_outfile = False
        tweetsubdirs = [ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ]
        cursordate = datetime.date(2008,8,8) # initializing to print progress
        for tweetsubdir in tweetsubdirs:
            subdirstr = tweetsubdir.split('/')[-1]
            print('SUBDIRSTR',subdirstr)
            new_outfile = self.out_more_tweets().path + '_subdirstr.json'
            print(tweetsubdir)
            # go through all tweet files
            tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
            date_tweets = defaultdict(list)
            for tweetfile in tweetfiles:
                # read in tweets
                with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                    tweetdicts = json.loads(file_in.read())
                tweets = []
                for td in tweetdicts:
                    tweetobj = tweet.Tweet()
                    tweetobj.import_tweetdict(td)
                    tweets.append(tweetobj)
                if len(tweets) > 0:
                    tweets_date = tweets[0].datetime.date()
                    if tweets_date != cursordate: # to print progress
                        print(tweets_date,'Queries:',date_term_events[tweets_date].keys())
                        cursordate = tweets_date
                    queries = date_term_events[tweets_date].keys()
                    for tweetobj in tweets:
                        matches = list(set(queries) & set(tweetobj.entities))
                        if len(matches) > 0:
                            for match in matches:
                                for eventobj in date_term_events[tweets_date][match]:
                                    eventobj.add_tweet(tweetobj)
            # count current status
            all_tweets = sum([len(eventobj.tweets) for eventobj in events])
            print('STATUS AFTER SUBDIR',tweetsubdir,':',all_tweets,'TWEETS IN TOTAL, NOW WRITING TO FILE')
            out_events = [eventobj.return_dict() for eventobj in events if eventobj.datetime.year == int(subdirstr[:4]) and eventobj.datetime.month == int(subdirstr[4:6])]
            with open(new_outfile,'w',encoding='utf-8') as file_out:
                json.dump(out_events,file_out)
            if last_outfile:
                os.system('rm ' + last_outfile)
            last_outfile = new_outfile

        # write new event file
        print('Done. Writing to file')
        out_events = [eventobj.return_dict() for eventobj in events]
        with open(self.out_more_tweets().path,'w',encoding='utf-8') as file_out:
            json.dump(out_events,file_out)
