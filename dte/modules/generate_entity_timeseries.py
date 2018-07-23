
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import re
import datetime 
from collections import defaultdict

from dte.functions import term_seeker
from dte.classes import tweet

################################################################################
###Timeseries generator
################################################################################

@registercomponent
class GetEntityTimeseries(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        timeseries_generator = workflow.new_task('get_entity_timeseries', GetEntityTimeseriesTask, autopass=True)
        timeseries_generator.in_tweetdir = input_feeds['tweetdir']
        timeseries_generator.in_events = input_feeds['events']

        return timeseries_generator

class GetEntityTimeseriesTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()

    def out_entity_counts(self):
        return self.outputfrominput(inputformat='tweetdir', stripextension='.tweets', addextension='.entity_counts')

    def run(self):

        # make counts directory
        self.setup_output_dir(self.out_entity_counts().path)

        # read in events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        # collect event entities
        entities = []
        for ed in eventdicts:
            entities.extend(ed['entities'])
        unique_entities = list(set(entities))

        # go through all tweet dirs
        tweetsubdirs = [ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ]
        for tweetsubdir in tweetsubdirs:
            print(tweetsubdir)
            # go through all tweet files
            tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
            date_tweets = defaultdict(list)
            print('Reading in tweets')
            for tweetfile in tweetfiles:
                print(tweetfile)
                # read in tweets
                with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                    tweetdicts = json.loads(file_in.read())
                for td in tweetdicts:
                    tweetobj = tweet.Tweet()
                    tweetobj.import_tweetdict(td)
                    date_tweets[tweetobj.datetime.date()].append(tweetobj)
            # make counts by date
            dates = date_tweets.keys()
            dates_sorted = sorted(dates)
            print('Counting terms')
            for date in dates_sorted:
                print(date)
                ts = term_seeker.TermSeeker()
                ts.set_tweets(date_tweets[date])
                ts.query_terms(unique_entities)
                # write to file
                with open(self.out_entity_counts().path + '/' + date.year + date.month + date.day + '.counts.json','w',encoding='utf-8') as file_out:
                    json.dump(ts.term_counts,file_out)


################################################################################
###Timeseries complementer
################################################################################
@registercomponent
class CountEntities(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()
    entity_counts = Parameter()

    date = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events'), InputFormat(self,format_id='entity_counts',extension='.entity_counts',inputparameter='entity_counts') ) ]

    def setup(self, workflow, input_feeds):

        entity_counter = workflow.new_task('count_entities', CountEntitiesTask, autopass=False, date=self.date)
        entity_counter.in_tweetdir = input_feeds['tweetdir']
        entity_counter.in_events = input_feeds['events']
        entity_counter.in_entity_counts = input_feeds['entity_counts']

        return entity_counter

class CountEntitiesTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    in_entity_counts = InputSlot()

    date = Parameter()

    def out_counts(self):
        return self.outputfrominput(inputformat='entity_counts', stripextension='.entity_counts', addextension='.entity_counts/' + self.date + '.counts.json')

    def run(self):

        #date_formatted = datetime.date(int(self.date[:4]),int(self.date[4:6]),int(self.date[6:]))

        # read in events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        # collect event entities
        entities = []
        for ed in eventdicts:
            entities.extend(ed['entities'])
        unique_entities = list(set(entities))

        # select tweetfiles of date
        tweetfiles = [tweetfile for tweetfile in glob.glob(self.in_tweetdir().path + '/' + self.date[:4] + self.date[4:6] + '/*') if re.search(self.date,tweetfile)]
        # go through all tweet files
        print('Reading in tweets')
        tweets = []
        for tweetfile in tweetfiles:
            # read in tweets
            with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                tweetdicts = json.loads(file_in.read())
            for td in tweetdicts:
                tweetobj = tweet.Tweet()
                tweetobj.import_tweetdict(td)
                tweets.append(tweetobj)

        # make counts
        print('Making counts')
        ts = term_seeker.TermSeeker()
        ts.set_tweets(tweets)
        ts.query_terms(unique_entities)

        # write to file
        with open(self.out_counts().path,'w',encoding='utf-8') as file_out:
            json.dump(ts.term_counts,file_out)
            
