
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

################################################################################
### Timeseries combiner
################################################################################

@registercomponent
class CombineEntityTimeseries(WorkflowComponent):

    entity_counts_dir = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='entity_counts_dir',extension='.timeseries',inputparameter='entity_counts_dir') ) ]

    def setup(self, workflow, input_feeds):

        timeseries_combiner = workflow.new_task('combine_entity_timeseries', CombineEntityTimeseriesTask, autopass=True)
        timeseries_combiner.in_entity_counts_dir = input_feeds['entity_counts_dir']

        return timeseries_combiner

class CombineEntityTimeseriesTask(Task):

    in_entity_counts_dir = InputSlot()

    def out_combined_counts(self):
        return self.outputfrominput(inputformat='entity_counts_dir', stripextension='.timeseries', addextension='.timeseries/combined.counts.npz')

    def out_combined_vocabulary(self):
        return self.outputfrominput(inputformat='entity_counts_dir', stripextension='.timeseries', addextension='.timeseries/combined.counts_vocabulary')

    def out_combined_dateseries(self):
        return self.outputfrominput(inputformat='entity_counts_dir', stripextension='.timeseries', addextension='.timeseries/combined.counts_dates')

    def run(self):

        # read entity counts
        print('Reading countfiles')
        countfiles = sorted([countfile for countfile in glob.glob(self.in_entity_counts_dir().path + '/20*' + 'counts.npz')])
        vocabularies = sorted([vocabulary for vocabulary in glob.glob(self.in_entity_counts_dir().path + '/20*' + 'counts_vocabulary')])
        datefiles = sorted([datesequence for datesequence in glob.glob(self.in_entity_counts_dir().path + '/20*' + 'counts_dates')])
        print(len(countfiles),'Countfiles and',len(vocabularies),'Vocabulary files and',len(datefiles),'datefiles')
        dates = []
        counts = []
        for j,countfile in enumerate(countfiles[:2]):
            print(countfile)
            with open(datefiles[j],'r',encoding='utf-8') as file_in:
                dates.extend(file_in.read().strip().split('\n'))
            loader = numpy.load(countfile)
            counts.append(sparse.csr_matrix((loader['data'], loader['indices'], loader['indptr']), shape = loader['shape']))
        with open(vocabularies[j],'r',encoding='utf-8') as file_in:
            vocabulary = file_in.read().strip().split('\n')
        print('Done. Vocabulary size:',len(vocabulary),'Num dates:',len(dates),'Shape first counts:',counts[0].shape)

        # combine counts
        print('Combining counts')
        counts_combined = sparse.hstack(counts)

        # write to files
        print('Writing to files')
        with open(self.out_combined_vocabulary().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(vocabulary))
        with open(self.out_combined_dateseries().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(dates))
        numpy.savez(self.out_combined_counts().path, data=counts_combined.data, indices=counts_combined.indices, indptr=counts_combined.indptr, shape=counts_combined.shape)        

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
        return [ ( InputFormat(self,format_id='tweetdir',extension='.tweets',inputparameter='tweetdir'), InputFormat(self,format_id='events',extension='.events.merged',inputparameter='events'), InputFormat(self,format_id='entity_counts',extension='.counts.npz',inputparameter='entity_counts'), InputFormat(self,format_id='entity_vocabulary',extension='.counts_vocabulary',inputparameter='entity_vocabulary'), InputFormat(self,format_id='entity_dates',extension='.counts_dates',inputparameter='entity_dates') ) ]

    def setup(self, workflow, input_feeds):

        entity_counter = workflow.new_task('count_entities', CountEntitiesDayTask, autopass=False, previous_date=self.previous_date, current_date=self.current_date)
        entity_counter.in_tweetdir = input_feeds['tweetdir']
        entity_counter.in_events = input_feeds['events']
        entity_counter.in_entity_counts = input_feeds['entity_counts']
        entity_counter.in_entity_vocabulary = input_feeds['entity_vocabulary']
        entity_counter.in_entity_dates = input_feeds['entity_dates']

        return entity_counter

class CountEntitiesDayTask(Task):

    in_tweetdir = InputSlot()
    in_events = InputSlot()
    in_entity_counts = InputSlot()
    in_entity_vocabulary = InputSlot()
    in_entity_dates = InputSlot()

    previous_date = Parameter()
    current_date = Parameter()

    def out_counts(self):
        return self.outputfrominput(inputformat='entity_counts', stripextension='.' + self.previous_date + '.counts.npz', addextension='.' + self.current_date + '.counts.npz')

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

        # read in entity_counts
        loader = numpy.load(self.in_entity_counts().path)
        counts = sparse.csr_matrix((loader['data'], loader['indices'], loader['indptr']), shape = loader['shape']))

        print('Setting entities')
        # collect event entities
        entities = []
        for ed in eventdicts:
            entities.extend(ed['entities'])
        unique_entities = list(set(entities) - set(vocabulary))
        if len(unique_entities) > 0:
            print('New entities:',' '.join(unique_entities).encode('utf-8'))
        set_vocabulary = set(vocabulary + unique_entities)

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
        term_zero = set(vocabulary + unique_entities) - set(date_entities_list)
        for term in term_zero:
            timeseries[term].append(0)
        for term in list(set(date_entities_list)):
            timeseries[term].append(date_entities[term])

        # append date to dateseries
        dates.append(self.current_date)        

        print('Done. Writing to files')
        timeseries_out_traditional = []
        timeseries_out_new = []
        for term in vocabulary:
            timeseries_out_traditional.append([timeseries[term]])
        timeseries_out_traditional_csr = sparse.csr_matrix(timeseries_out_traditional)
        print('Shape original counts:',counts.shape,'Shape new column:',timeseries_out_traditional_csr.shape)
        new_counts = sparse.hstack(counts,timeseries_out_csr)
        print('Vocabulary length before',len(vocabulary))
        for term in unique_entities:
            vocabulary.append(term)
            timeseries_out_new.append(([0] * counts.shape[1]) + [timeseries[term]])
        print('Vocabulary length after',len(vocabulary))
        timeseries_out_new_csr = sparse.csr_matrix(timeseries_out_new)
        final_counts = sparse.vstack(new_counts,timeseries_out_new_csr)
        print('Shape original counts:',counts.shape,'Shape new rows:',timeseries_out_new_csr.shape,'Shape final counts:',final_counts.shape)
        numpy.savez(self.out_counts().path, data=final_counts.data, indices=final_counts.indices, indptr=final_counts.indptr, shape=final_counts.shape)        
        with open(self.out_vocabulary().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(vocabulary))
        with open(self.out_dates().path,'w',encoding='utf-8') as out:
            out.write('\n'.join(dates))
