
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import re
import datetime 
from collections import defaultdict
import numpy
from scipy import sparse

################################################################################
###Timeseries generator
################################################################################

@registercomponent
class GetEntityTimeseriesMonth(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()

    month = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        timeseries_generator = workflow.new_task('get_entity_timeseries', GetEntityTimeseriesTask, autopass=True, month=self.month)
        timeseries_generator.in_tweetdir = input_feeds['tweetdir']
        timeseries_generator.in_events = input_feeds['events']

        return timeseries_generator
    
class GetEntityTimeseriesTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    
    month = Parameter()

    def out_entity_counts(self):
        return self.outputfrominput(inputformat='tweetdir', stripextension='.tweets', addextension='.timeseries/' + self.month + '.counts.npz')

    def out_vocabulary(self):
        return self.outputfrominput(inputformat='tweetdir', stripextension='.tweets', addextension='.timeseries/' + self.month + '.counts_vocabulary')

    def out_dateseries(self):
        return self.outputfrominput(inputformat='tweetdir', stripextension='.tweets', addextension='.timeseries/' + self.month + '.counts_dates')

    def run(self):

        timeseries = defaultdict(list)
        dateseries = []

        # read in events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        # collect event entities
        entities = []
        for ed in eventdicts:
            entities.extend(ed['entities'])
        unique_entities = set(entities)
        eventdicts = None # to save memory
        tweetsubdir = self.in_tweetdir().path + '/' + self.month
        # go through all tweet files
        tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
        cursordate = False
        date_entities = defaultdict(int)
        date_entities_list = []
        print('Reading in tweets')
        for tweetfile in tweetfiles:
            print(tweetfile)
            tweetfile_datestr = tweetfile.split('/')[-1].split('.')[0].split('-')[0]
            filedate = datetime.date(int(tweetfile_datestr[:4]),int(tweetfile_datestr[4:6]),int(tweetfile_datestr[6:8]))
            if not cursordate:
                cursordate = filedate
            if filedate > cursordate:
                dateseries.append(''.join(str(cursordate).split('-')))
                # integrate in timeseries
                print(cursordate)
                term_zero = unique_entities - set(date_entities_list)
                for term in term_zero:
                    timeseries[term].append(0)
                for term in list(set(date_entities_list)):
                    timeseries[term].append(date_entities[term])
                cursordate = filedate
                date_entities = defaultdict(int)
                date_entities_list = []
            # read in tweets
            with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                tweetdicts = json.loads(file_in.read())
            for td in tweetdicts:
                for term in (unique_entities & set(list(td['entities'].keys()))):
                    date_entities[term] += 1
                    date_entities_list.append(term)
                                                   
        dateseries.append(''.join(str(cursordate).split('-')))
        # integrate in timeseries
        print(cursordate)
        term_zero = unique_entities - set(date_entities_list)
        for term in term_zero:
            timeseries[term].append(0)
        for term in list(set(date_entities_list)):
            timeseries[term].append(date_entities[term])
                
        print('Done. Writing to files')
        vocabulary = sorted(list(unique_entities))
        with open(self.out_vocabulary().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(vocabulary))

        with open(self.out_dateseries().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(dateseries))

        timeseries_out = []
        for term in vocabulary:
            timeseries_out.append(timeseries[term])
        timeseries_csr = sparse.csr_matrix(timeseries_out)
        numpy.savez(self.out_entity_counts().path, data=timeseries_csr.data, indices=timeseries_csr.indices, indptr=timeseries_csr.indptr, shape=timeseries_csr.shape)        
        
        # for entity in unique_entities:
        #     timeseries_out.append(' '.join([str(x) for x in timeseries[entity]]))
        # with gzip.open(self.out_entity_counts().path,'wb') as out:
        #     out.write('\n'.join(timeseries_out).encode('utf-8'))

################################################################################
###Timeseries complementer
################################################################################
@registercomponent
class CountEntitiesDay(WorkflowComponent):

    tweetdir = Parameter()
    events = Parameter()
    entity_counts = Parameter()
    entity_vocabulary = Parameter()
    entity_dates = Parameter()

    previous_date = Parameter()
    current_date = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events.merged',inputparameter='events'), InputFormat(self,format_id='entity_counts',extension='.counts.gzip',inputparameter='entity_counts'), InputFormat(self,format_id='entity_vocabulary',extension='.counts_vocabulary',inputparameter='entity_vocabulary'), InputFormat(self,format_id='entity_dates',extension='.counts_dates',inputparameter='entity_dates') ) ]

    def setup(self, workflow, input_feeds):

        entity_counter = workflow.new_task('count_entities', CountEntitiesTask, autopass=False, previous_date=self.previous_date, current_date=self.current_date)
        entity_counter.in_tweetdir = input_feeds['tweetdir']
        entity_counter.in_events = input_feeds['events']
        entity_counter.in_entity_counts = input_feeds['entity_counts']
        entity_counter.in_entity_vocabulary = input_feeds['entity_vocabulary']
        entity_counter.in_entity_dates = input_feeds['entity_dates']

        return entity_counter

class CountEntitiesTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    in_entity_counts = InputSlot()
    in_entity_vocabulary = InputSlot()
    in_entity_dates = InputSlot()

    previous_date = Parameter()
    current_date = Parameter()

    def out_counts(self):
        return self.outputfrominput(inputformat='entity_counts', stripextension='.' + self.previous_date + '.counts.gzip', addextension='.' + self.current_date + '.counts.gzip')

    def out_vocabulary(self):
        return self.outputfrominput(inputformat='entity_vocabulary', stripextension='.' + self.previous_date + '.counts_vocabulary', addextension='.' + self.current_date + '.counts_vocabulary')

    def out_dates(self):
        return self.outputfrominput(inputformat='entity_dates', stripextension='.' + self.previous_date + '.counts_dates', addextension='.' + self.current_date + '.counts_dates')

    def run(self):

        timeseries = defaultdict(list)

        # read files
        print('Loading data')
        # read events
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        
        # read vocabulary
        with open(self.in_entity_vocabulary().path,'r',encoding='utf-8') as file_in:
            vocabulary = file_in.read().strip().split('\n')

        # read datesequence
        with open(self.in_entity_dates().path,'r',encoding='utf-8') as file_in:
            dates = file_in.read().strip().split('\n')

        # read in gzipped entity_counts
        for i,line in enumerate(io.TextIOWrapper(io.BufferedReader(gzip.open(self.in_entity_counts().path)), encoding='utf-8')):
            timeseries[vocabulary[i]] = line.split()

        print('Setting entities')
        # collect event entities
        entities = []
        for ed in eventdicts:
            entities.extend(ed['entities'])
        unique_entities = list(set(entities) - set(vocabulary))
        if len(unique_entities) > 0:
            print('New entities:',' '.join(unique_entities).encode('utf-8'))
        vocabulary.extend(unique_entities)
        set_vocabulary = set(vocabulary)

        # select tweetfiles of date
        tweetfiles = [tweetfile for tweetfile in glob.glob(self.in_tweetdir().path + '/' + self.current_date[:4] + self.current_date[4:6] + '/*') if re.search(self.current_date,tweetfile)]
        # go through all tweet files
        print('Reading in tweets')
        date_entities = defaultdict(int)
        date_entities_list = []
        for tweetfile in tweetfiles:
            print(tweetfile)
            # read in tweets
            with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                tweetdicts = json.loads(file_in.read())
            for td in tweetdicts:
                for term in (set_vocabulary & set(list(td['entities'].keys()))):
                    date_entities[term] += 1
                    date_entities_list.append(term)

        # append counts to timeseries
        print('Saving counts')
        term_zero = set(vocabulary) - set(date_entities_list)
        for term in term_zero:
            timeseries[term].append(0)
        for term in list(set(date_entities_list)):
            timeseries[term].append(date_entities[term])

        # append date to dateseries
        dates.append(self.current_date)

        print('Done. Writing to files')
        with open(self.out_vocabulary().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(vocabulary))

        with open(self.out_dates().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(dates))

        timeseries_out = []
        for entity in vocabulary:
            timeseries_out.append(' '.join([str(x) for x in timeseries[entity]]))
        with gzip.open(self.out_counts().path,'wb') as out:
            out.write('\n'.join(timeseries_out).encode('utf-8'))
