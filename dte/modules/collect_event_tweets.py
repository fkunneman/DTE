
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

        tweet_collector = workflow.new_task('collect_event_tweets', CollectEventTweetsTask, autopass=False)
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

        # read in events
        term_events = defaultdict(list)
        print('Reading in events')
        extended_events =[]
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
            for ed in eventdicts:
                eventobj = event.Event()
                eventobj.import_eventdict(ed)
                extended_events.append(eventobj)
                if len(list(set_bursty_entities & set(list(ed['entities'])))) > 0:
                    for term in list(set_bursty_entities & set(list(ed['entities']))):
                        term_events[term].append(eventobj)

        # for each entity
        print('Saving event tweets dates by entity')
        date_entity_events = defaultdict(lambda : defaultdict(list))
        for entity in bursty_entities:
            print('Entity',entity.encode('utf-8'))
            # for each event
            if len(term_events[entity]) == 0:
                continue
            #elif len(term_events[entity]) == 1:
                #ev = term_events[entity][0]
                # event_date_str = ''.join(str(ev.datetime).split()[0].split('-'))
                # try:
                #     event_date_index = dates.index(event_date)
                # except:
                #     continue
                # first = event_date_index - 100 if (event_date_index - 100) >= 0 else 0
                # last = event_date_index + 100 if (event_date_index + 100) < len(dates) else len(dates)-1
                # first = ev.datetime - datetime.timedelta(days=100)
                # last = ev.datetime + datetime.timedelta(days=100)
                # cursor = first
                # while cursor <= last:
                #     date_str = ''.join(str(cursor).split()[0].split('-'))
                #     date_entity_event[date_str][entity] = ev
                #     cursor += datetime.timedelta(days=1)
                #tweets = sum(entity_tweetsequence[entity][first:last],[])
                #ev.add_tweets(tweets)
            else:
                # events = term_events[entity]
                # event_dates = [''.join(str(ev.datetime).split()[0].split('-')) for ev in events]
                # try:
                #     event_date_indices = [dates.index(event_date) for event_date in event_dates]
                # except:
                #     continue
                for i,ev in enumerate(term_events[entity]):
                    #index = event_date_indices[i]
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

                    #     first = ev.datetime - datetime.timedelta(days=100)
                    #     first = index - 100 if (index - 100) >= 0 else 0
                    # else:
                    #     minus = 100 if index - event_date_indices[i-1] > 199 else ((index - event_date_indices[i-1]) / 2) 
                    #     first = index - minus
                    # if i == len(events)-1:
                    #     last = index + 100 if (event_date_index + 100) < len(dates) else len(dates)-1
                    # else:
                    #     plus = 100 if event_date_indices[i+1] - index > 199 else ((event_date_indices[i-1] - index) / 2) 
                    #     last = index + plus
                    #     tweets = sum(entity_tweetsequence[entity][first:last],[])
                    #     ev.add_tweets(tweets)

        # read in tweets
        print('Collecting additional tweets')
        tweetsubdirs = sorted([ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ])
        cursordate = '20070707' # initializing to print progress
        entity_tweets = defaultdict(list)
        first = True
        candidate_entities = []
        for tweetsubdir in tweetsubdirs[:12]:
            subdirstr = tweetsubdir.split('/')[-1]
            print('SUBDIRSTR',subdirstr)
            # go through all tweet files
            tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
            for tweetfile in tweetfiles:
                print(tweetfile)
                datestr = tweetfile.split('/')[-1].split('.')[0].split('-')[0]
                if first:
                    candidate_entities = list(date_entity_event[datestr].keys())
                    set_candidate_entities = set(candidate_entities)
                    cursordate = datestr
                    first = False
                elif datestr != cursordate:
                    # add tweets
                    for entity in candidate_entities:
                        for ev in date_entity_events[datestr][entity]:
                            ev.add_tweets(entity_tweets[entity])
                    cursordate = datestr
                    candidate_entities = list(date_entity_event[datestr].keys())
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

# @registercomponent
# class CollectEventTweetsDaily(WorkflowComponent):

#     tweetdir = Parameter()
#     events = Parameter()
#     events_new = Parameter()
#     entity_burstiness = Parameter()
#     entity_burstiness_new = Parameter()

#     date = Parameter() 

#     def accepts(self):
#         return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events'), InputFormat(self,format_id='events_new',extension='.merged',inputparameter='events_new'), InputFormat(self,format_id='entity_burstiness',extension='.burstiness.txt',inputparameter='entity_burstiness'), InputFormat(self,format_id='entity_burstiness_new',extension='.burstiness.txt',inputparameter='entity_burstiness_new') ) ]

#     def setup(self, workflow, input_feeds):

#         daily_tweet_collector = workflow.new_task('collect_event_tweets_daily', CollectEventTweetsDailyTask, autopass=False, date=self.date)
#         daily_tweet_collector.in_tweetdir = input_feeds['tweetdir']
#         daily_tweet_collector.in_events = input_feeds['events']
#         daily_tweet_collector.in_events_new = input_feeds['events_new']
#         daily_tweet_collector.in_entity_burstiness = input_feeds['entity_burstiness']
#         daily_tweet_collector.in_entity_burstiness_new = input_feeds['entity_burstiness_new']

#         return daily_tweet_collector

# class CollectEventTweetsDailyTask(Task):

#     in_tweetdir = InputSlot()
#     in_events = InputSlot()
#     in_events_new = InputSlot()
#     in_entity_burstiness = InputSlot()
#     in_entity_burstiness_new = InputSlot()

#     date = Parameter()

#     def out_more_tweets(self):
#         return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.more_tweets.events')

#     def run(self):

#         # read in burstiness
#         print('Reading bursty entities')
#         with open(self.in_entity_burstiness().path,'r',encoding='utf-8') as file_in:
#             bursty_entities = file_in.read().strip().split('\n')
#         set_bursty_entities = set(bursty_entities)

#         print('Reading new bursty entities')
#         with open(self.in_entity_burstiness_new().path,'r',encoding='utf-8') as file_in:
#             new_bursty_entities = file_in.read().strip().split('\n')
#         set_bursty_entities_new = set(bursty_entities_new)
#         bursty_entities_new_unique = list(set_bursty_entities_new - set_bursty_entities)

#         # read in tweets
#         print('Reading new tweets')
#         tweetsubdirs = sorted([ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ])
#         cursordate = '20070707' # initializing to print progress
#         entity_tweetsequence = defaultdict(list)
#         dates = []
#         entities_date = []
#         entity_tweets = defaultdict(list)
#         for tweetsubdir in tweetsubdirs[:12]:
#             subdirstr = tweetsubdir.split('/')[-1]
#             print('SUBDIRSTR',subdirstr)
#             new_outfile = self.out_more_tweets().path + '_' + subdirstr + '.json'
#             # go through all tweet files
#             tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
#             for tweetfile in tweetfiles:
#                 print(tweetfile)
#                 datestr = tweetfile.split('/')[-1].split('.')[0].split('-')[0]
#                 if datestr != cursordate:
#                     for entity in list(set_bursty_entities - set(entities_date)):
#                         entity_tweetsequence[entity].append([])
#                     for entity in list(set(entities_date)):
#                         entity_tweetsequence[entity].append(entity_tweets[entity])
#                     dates.append(cursordate)
#                     cursordate = datestr
#                     entities_date = []
#                     entity_tweets = defaultdict(list)
#                 # read in tweets
#                 with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
#                     tweetdicts = json.loads(file_in.read())
#                 for td in tweetdicts:
#                     if len(list(set_bursty_entities & set(list(td['entities'].keys())))) > 0:
#                         tweetobj = tweet.Tweet()
#                         tweetobj.import_tweetdict(td)
#                     for term in list(set_bursty_entities & set(list(td['entities'].keys()))):
#                         entity_tweets[term].append(tweetobj)
#                         entities_date.append(term)
#         for entity in list(set_bursty_entities - set(entities_date)):
#             entity_tweetsequence[entity].append([])
#         for entity in list(set(entities_date)):
#             entity_tweetsequence[entity].append(entity_tweets[entity])
#         dates.append(cursordate)

#         # read in events
#         term_events = defaultdict(list)
#         print('Reading in events')
#         extended_events =[]
#         with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
#             eventdicts = json.loads(file_in.read())
#             for ed in eventdicts:
#                 eventobj = event.Event()
#                 eventobj.import_eventdict(ed)
#                 extended_events.append(eventobj)
#                 if len(list(set_bursty_entities & set(list(ed['entities'])))) > 0:
#                     for term in list(set_bursty_entities & set(list(ed['entities']))):
#                         term_events[term].append(eventobj)

#         # for each entity
#         print('Adding event tweets by entity')
#         for entity in bursty_entities:
#             print('Entity',entity.encode('utf-8'))
#             # for each event
#             if len(term_events[entity]) == 0:
#                 continue
#             elif len(term_events[entity]) == 1:
#                 ev = term_events[entity][0]
#                 event_date = ''.join(str(ev.datetime).split()[0].split('-'))
#                 try:
#                     event_date_index = dates.index(event_date)
#                 except:
#                     continue
#                 first = event_date_index - 100 if (event_date_index - 100) >= 0 else 0
#                 last = event_date_index + 100 if (event_date_index + 100) < len(dates) else len(dates)-1
#                 tweets = sum(entity_tweetsequence[entity][first:last],[])
#                 ev.add_tweets(tweets)
#             else:
#                 events = term_events[entity]
#                 event_dates = [''.join(str(ev.datetime).split()[0].split('-')) for ev in events]
#                 try:
#                     event_date_indices = [dates.index(event_date) for event_date in event_dates]
#                 except:
#                     continue
#                 for i,ev in enumerate(events):
#                     index = event_date_indices[i]
#                     if i == 0:
#                         first = index - 100 if (index - 100) >= 0 else 0
#                     else:
#                         minus = 100 if index - event_date_indices[i-1] > 199 else ((index - event_date_indices[i-1]) / 2) 
#                         first = index - minus
#                     if i == len(events)-1:
#                         last = index + 100 if (event_date_index + 100) < len(dates) else len(dates)-1
#                     else:
#                         plus = 100 if event_date_indices[i+1] - index > 199 else ((event_date_indices[i-1] - index) / 2) 
#                         last = index + plus
#                         tweets = sum(entity_tweetsequence[entity][first:last],[])
#                         ev.add_tweets(tweets)

#         # write to file
#         print('Writing new events')
#         out_extended_events = [ev.return_dict() for ev in extended_events]
#         with open(self.out_more_tweets().path,'w',encoding='utf-8') as file_out:
#             json.dump(out_extended_events,file_out)
