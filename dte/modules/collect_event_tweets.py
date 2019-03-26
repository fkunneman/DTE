
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

    first_event_date = Parameter()
    last_event_date = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events'), InputFormat(self,format_id='entity_burstiness',extension='.burstiness.txt',inputparameter='entity_burstiness') ) ]

    def setup(self, workflow, input_feeds):

        tweet_collector = workflow.new_task('collect_event_tweets', CollectEventTweetsTask, autopass=False, first_event_date=self.first_event_date, last_event_date=self.last_event_date)
        tweet_collector.in_tweetdir = input_feeds['tweetdir']
        tweet_collector.in_events = input_feeds['events']
        tweet_collector.in_entity_burstiness = input_feeds['entity_burstiness']

        return tweet_collector

class CollectEventTweetsTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    in_entity_burstiness = InputSlot()

    first_event_date = Parameter()
    last_event_date = Parameter()

    def out_more_tweets(self):
        return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.more_tweets.' + self.first_event_date + '-' + self.last_event_date + '.events')

    def run(self):

        first_event_date_dt = datetime.datetime(int(self.first_event_date[:4]),int(self.first_event_date[4:6]),int(self.first_event_date[6:]))
        last_event_date_dt = datetime.datetime(int(self.last_event_date[:4]),int(self.last_event_date[4:6]),int(self.last_event_date[6:]))    
        
        # read in burstiness
        print('Reading in bursty entities')
        with open(self.in_entity_burstiness().path,'r',encoding='utf-8') as file_in:
            bursty_entities = file_in.read().strip().split('\n')
        set_bursty_entities = set(bursty_entities)

        # read in events
        term_events = defaultdict(list)
        print('Reading in events')
        extended_events = []
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
            for i,ed in enumerate(eventdicts):
                eventobj = event.Event()
                eventobj.import_eventdict(ed)
                if eventobj.datetime >= first_event_date_dt - datetime.timedelta(days=100) and eventobj.datetime <= last_event_date_dt + datetime.timedelta(days=100):
                    extended_events.append(eventobj)
                    if len(list(set_bursty_entities & set(list(ed['entities'])))) > 0:
                        for term in list(set_bursty_entities & set(list(ed['entities']))):
                            term_events[term].append(eventobj)

        # for each entity
        print('Saving event tweets dates by entity')
        date_entity_events = defaultdict(lambda : defaultdict(list))
        for entity in bursty_entities:
            # for each event
            if len(term_events[entity]) == 0:
                continue
            else:
                for i,ev in enumerate(term_events[entity]):
                    if ev.datetime >= first_event_date_dt and ev.datetime <= last_event_date_dt:
                        if i == 0:
                           minus = 100
                        else:
                           minus = 100 if ((ev.datetime - term_events[entity][i-1].datetime).days > 199 or (ev.datetime - term_events[entity][i-1].datetime).days < 3) else ((ev.datetime - term_events[entity][i-1].datetime).days) / 2
                        if i == len(term_events[entity])-1:
                           plus = 100
                        else:
                           plus = 100 if ((term_events[entity][i-1].datetime - ev.datetime).days > 199 or (term_events[entity][i-1].datetime - ev.datetime).days < 3) else ((term_events[entity][i-1].datetime - ev.datetime).days) / 2
                        first = ev.datetime - datetime.timedelta(days=minus)
                        last = ev.datetime + datetime.timedelta(days=plus)
                        cursor = first
                        while cursor <= last:
                            date_str = ''.join(str(cursor).split()[0].split('-'))
                            date_entity_events[date_str][entity].append(ev)
                            cursor += datetime.timedelta(days=1)

        # read in tweets
        print('Collecting additional tweets')
        dates = list(date_entity_events.keys())
        months = list(set([d[:6] for d in dates]))
        tweetsubdirs = sorted([ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ])
        entity_tweets = defaultdict(list)
        first = True
        for tweetsubdir in tweetsubdirs:
            subdirstr = tweetsubdir.split('/')[-1]
            if subdirstr in months:
                # go through all tweet files
                tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
                for tweetfile in tweetfiles:
                    print(tweetfile)
                    datestr = tweetfile.split('/')[-1].split('.')[0].split('-')[0]
                    if datestr in dates:
                        if first:
                            candidate_entities = list(date_entity_events[datestr].keys())
                            set_candidate_entities = set(candidate_entities)
                            cursordate = datestr
                            first = False
                        elif datestr != cursordate:
                            # add tweets
                            for entity in candidate_entities:
                                for ev in date_entity_events[datestr][entity]:
                                    ev.add_tweets(entity_tweets[entity])
                            cursordate = datestr
                            candidate_entities = list(date_entity_events[datestr].keys())
                            set_candidate_entities = set(candidate_entities)
                            entity_tweets = defaultdict(list)
                        # read in tweets
                        with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                            tweetdicts = json.loads(file_in.read())
                        for td in tweetdicts:
                            if len(list(set_candidate_entities & set(list(td['entities'].keys())))) > 0:
                                tweetobj = tweet.Tweet()
                                tweetobj.import_tweetdict(td)
                            for term in list(set_candidate_entities & set(list(td['entities'].keys()))):
                                entity_tweets[term].append(tweetobj)

        # write to file
        print('Writing new events')
        out_extended_events = [ev.return_dict() for ev in extended_events]
        with open(self.out_more_tweets().path,'w',encoding='utf-8') as file_out:
            json.dump(out_extended_events,file_out)


#########################################
### New events
#########################################

@registercomponent
class CollectEventTweetsDaily(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()
    entity_burstiness = Parameter()
    entity_burstiness_new = Parameter()

    date = Parameter() 

    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events.integrated',inputparameter='events'), InputFormat(self,format_id='entity_burstiness',extension='.burstiness.txt',inputparameter='entity_burstiness'), InputFormat(self,format_id='entity_burstiness_new',extension='.burstiness.txt',inputparameter='entity_burstiness_new') ) ]

    def setup(self, workflow, input_feeds):

        daily_tweet_collector = workflow.new_task('collect_event_tweets_daily', CollectEventTweetsDailyTask, autopass=False, date=self.date)
        daily_tweet_collector.in_tweetdir = input_feeds['tweetdir']
        daily_tweet_collector.in_events = input_feeds['events']
        daily_tweet_collector.in_entity_burstiness = input_feeds['entity_burstiness']
        daily_tweet_collector.in_entity_burstiness_new = input_feeds['entity_burstiness_new']

        return daily_tweet_collector

class CollectEventTweetsDailyTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    in_entity_burstiness = InputSlot()
    in_entity_burstiness_new = InputSlot()

    date = Parameter()

    def out_more_tweets(self):
        return self.outputfrominput(inputformat='events', stripextension='.events.integrated', addextension='.more_tweets.events.integrated')

    def run(self):

        # read in burstiness
        print('Reading bursty entities')
        with open(self.in_entity_burstiness().path,'r',encoding='utf-8') as file_in:
            bursty_entities = file_in.read().strip().split('\n')
        set_bursty_entities = set(bursty_entities)

        print('Reading new bursty entities')
        with open(self.in_entity_burstiness_new().path,'r',encoding='utf-8') as file_in:
            new_bursty_entities = file_in.read().strip().split('\n')
        set_bursty_entities_new = set(new_bursty_entities)
        bursty_entities_new_unique = list(set_bursty_entities_new - set_bursty_entities)

        # read in events
        term_events = defaultdict(list)
        term_novel_events = defaultdict(list)
        date_dt = datetime.datetime(int(date[:4]),int(date[4:6]),int(date[6:8]))
        event_bound = date_dt - datetime.timedelta(days=100)
        print('Reading in events')
        extended_events = []
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
            for i,ed in enumerate(eventdicts):
                eventobj = event.Event()
                eventobj.import_eventdict(ed)
                extended_events.append(eventobj)
                if eventobj.status == 'novel':
                    if len(list(set_bursty_entities_new & set(list(ed['entities'])))) > 0:
                        for term in list(set_bursty_entities_new & set(list(ed['entities']))):
                            term_novel_events[term].append(eventobj)
                if eventobj.datetime >= event_bound:
                    if len(list(set_bursty_entities & set(list(ed['entities'])))) > 0:
                        for term in list(set_bursty_entities & set(list(ed['entities']))):
                            term_events[term].append(eventobj)

        date_entity_events = defaultdict(lambda : defaultdict(list))
        for entity in bursty_entities_new_unique:
            # for each event
            if len(term_events[entity]) == 0:
                continue
            else:
                for i,ev in enumerate(term_events[entity]):
                    if i == 0:
                        minus = 100
                    else:
                        minus = 100 if ((ev.datetime - term_events[entity][i-1].datetime).days > 199 or (ev.datetime - term_events[entity][i-1].datetime).days < 3) else ((ev.datetime - term_events[entity][i-1].datetime).days) / 2
                    if i == len(term_events[entity])-1:
                        plus = 100
                    else:
                        plus = 100 if ((term_events[entity][i-1].datetime - ev.datetime).days > 199 or (term_events[entity][i-1].datetime - ev.datetime).days < 3) else ((term_events[entity][i-1].datetime - ev.datetime).days) / 2
                    first = ev.datetime - datetime.timedelta(days=minus)
                    last = ev.datetime + datetime.timedelta(days=plus)
                    cursor = first
                    while cursor <= last:
                        date_str = ''.join(str(cursor).split()[0].split('-'))
                        date_entity_events[date_str][entity].append(ev)
                        cursor += datetime.timedelta(days=1)

        for entity in new_bursty_entities:
            # for each event
            if len(term_novel_events[entity]) == 0:
                continue
            else:
                for i,ev in enumerate(term_novel_events[entity]):
                    if i == 0:
                        minus = 100
                    else:
                        minus = 100 if ((ev.datetime - term_novel_events[entity][i-1].datetime).days > 199 or (ev.datetime - term_novel_events[entity][i-1].datetime).days < 3) else ((ev.datetime - term_novel_events[entity][i-1].datetime).days) / 2
                    if i == len(term_novel_events[entity])-1:
                        plus = 100
                    else:
                        plus = 100 if ((term_novel_events[entity][i-1].datetime - ev.datetime).days > 199 or (term_novel_events[entity][i-1].datetime - ev.datetime).days < 3) else ((term_novel_events[entity][i-1].datetime - ev.datetime).days) / 2
                    first = ev.datetime - datetime.timedelta(days=minus)
                    last = ev.datetime + datetime.timedelta(days=plus)
                    cursor = first
                    while cursor <= last:
                        date_str = ''.join(str(cursor).split()[0].split('-'))
                        date_entity_events[date_str][entity].append(ev)
                        cursor += datetime.timedelta(days=1)
                        
        # read in tweets
        print('Collecting additional tweets')
        dates = list(date_entity_events.keys())
        months = list(set([d[:6] for d in dates]))
        tweetsubdirs = sorted([subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ])
        entity_tweets = defaultdict(list)
        first = True
        for tweetsubdir in tweetsubdirs:
            subdirstr = tweetsubdir.split('/')[-1]
            if subdirstr in months:
                # go through all tweet files
                tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
                for tweetfile in tweetfiles:
                    print(tweetfile)
                    datestr = tweetfile.split('/')[-1].split('.')[0].split('-')[0]
                    if datestr in dates or datestr == self.date:
                        if first:
                            candidate_entities = list(date_entity_events[datestr].keys())
                            set_candidate_entities = set(candidate_entities)
                            cursordate = datestr
                            first = False
                        elif datestr != cursordate:
                            # add tweets
                            for entity in candidate_entities:
                                for ev in date_entity_events[datestr][entity]:
                                    ev.add_tweets(entity_tweets[entity])
                                    ev.status = 'changed'
                            cursordate = datestr
                            candidate_entities = list(date_entity_events[datestr].keys())
                            set_candidate_entities = set(candidate_entities)
                            entity_tweets = defaultdict(list)
                        # read in tweets
                        with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                            tweetdicts = json.loads(file_in.read())
                        for td in tweetdicts:
                            if datestr == self.date:
                                match = set_bursty_entities_new
                            else:
                                match = set_candidate_entities
                            if len(list(match & set(list(td['entities'].keys())))) > 0:
                                tweetobj = tweet.Tweet()
                                tweetobj.import_tweetdict(td)
                            for term in list(match & set(list(td['entities'].keys()))):
                                entity_tweets[term].append(tweetobj)

        # add final tweets
        for entity in candidate_entities:
            for ev in date_entity_events[datestr][entity]:
                ev.add_tweets(entity_tweets[entity])
                ev.status = 'changed'
            
        # write to file
        print('Writing new events')
        out_extended_events = [ev.return_dict(txt=False) for ev in extended_events]
        with open(self.out_more_tweets().path,'w',encoding='utf-8') as file_out:
            json.dump(out_extended_events,file_out)
