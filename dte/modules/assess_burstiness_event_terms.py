
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict
from scipy import sparse
import numpy

from dte.functions import burstiness_detector
from dte.classes import event


################################################################################
###Periodicity detector
################################################################################

@registercomponent
class AssessBurstiness(WorkflowComponent):

    entity_counts_dir = Parameter()
    events = Parameter()

    burstiness_threshold = Parameter()
    date = Parameter()

    def accepts(self):
        return [ ( InputFormat(self,format_id='entity_counts_dir',extension='.timeseries',inputparameter='entity_counts_dir'), InputFormat(self,format_id='events',extension='.events',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        burstiness_assessor = workflow.new_task('assess_burstiness', AssessBurstinessTask, autopass=False, burstiness_threshold=self.burstiness_threshold,date=self.date)
        burstiness_assessor.in_entity_counts_dir = input_feeds['entity_counts_dir']
        burstiness_assessor.in_events = input_feeds['events']

        return burstiness_assessor

class AssessBurstinessTask(Task):

    in_entity_counts_dir = InputSlot()
    in_events = InputSlot()

    burstiness_threshold = Parameter()
    date = Parameter()

    def out_stats(self):
        return self.outputfrominput(inputformat='entity_counts_dir', stripextension='.timeseries', addextension='.burstiness/' + self.date + '.stats.txt')

    def out_bursty_entities(self):
        return self.outputfrominput(inputformat='entity_counts_dir', stripextension='.timeseries', addextension='.burstiness/' + self.date + '.bursty_entities.txt')

    def run(self):

        # read in entity counts
        print('Reading countfiles')
        countfiles = sorted([countfile for countfile in glob.glob('lamaevents.timeseries/20*' + 'counts.gz')])
        vocabularies = sorted([vocabulary for vocabulary in glob.glob('lamaevents.timeseries/20*' + 'counts_vocabulary')])
        dates = sorted([datesequence for datesequence in glob.glob('lamaevents.timeseries/20*' + 'counts_dates')])
        print(len(countfiles),'Countfiles and',len(vocabularies),'Vocabulary files and',len(dates),'datefiles')
        counts = defaultdict(list)
        dates = []
        for j,countfile in enumerate(countfiles):
            with open(dates[j],'r',encoding='utf-8') as file_in:
                dates.extend(file_in.read().strip().split('\n'))
            with open(vocabularies[j],'r',encoding='utf-8') as file_in:
                vocabulary = file_in.read().strip().split('\n')
            for i,line in enumerate(io.TextIOWrapper(io.BufferedReader(gzip.open(countfile)), encoding='utf-8')):
                counts[vocabulary[i]].extend(line.split())
        counts_matrix = []
        matrix_vocabulary = []
        for k,v in counts.items()
            matrix_vocabulary.append(k)
            counts_matrix.append(v)
        counts_csr = sparse.csr_matrix(counts_matrix)
        print('Counts shape:',counts_csr.shape)

        # read in events
        print('Reading events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        
        # collect event entities
        print('Indexing entities')
        entity_dates = defaultdict(list)
        for ed in eventdicts:
            eventdate = ''.join(ed['datetime'].split()[0].split('-'))
            for entity in ed['entities']:
                entity_dates[entity].append(eventdate)

        # assess burstiness 
        print('Assessing burstiness')
        cursor = range(0,counts_csr.shape[0],1000)
        terms = []
        term_burstiness = []
        term_burstiness_stats = []
        for i,term in enumerate(matrix_vocabulary):
            if i in cursor:
                print('Term',i,'of',counts_csr.shape[0])
            burstiness = []
            burstiness_stats = []
            for date in entity_dates[term]:
                index = dates.index(date)
                min_index = index - 30 if (index-30 >= 0) else 0
                max_index = index + 30 if (index+30 <= counts_csr.shape[1]) else counts_csr.shape[1]
                if max_index - min_index > 40:
                    # if term occurs more often than 10 times
                    if counts_csr[i,min_index:max_index].sum() > 10:
                        burstiness.append(counts_csr[i,index] / counts_csr[i,min_index:max_index].mean())
                        burstiness_stats.append([counts_csr[i,index],counts_csr[i,min_index:max_index].mean()])
            if len(burstiness) > 0:
                avg_burstiness = numpy.mean(burstiness)
                if avg_burstiness > self.burstiness_threshold:
                    term_burstiness.append(1)
                else:
                    term_burstiness.append(0)
                term_burstiness_stats.append('****'.join([term,str(avg_burstiness),'-!-'.join(['-'.join([str(x) for x in y]) for y in burstiness_stats])])) 
            else:
                term_burstiness.append(0)
                term_burstiness_stats.append('Frequency too low')
        bursty_entities = numpy.array(matrix_vocabulary)[numpy.array(term_burstiness).nonzero()].tolist()

        # write burstiness
        print('Done. Writing to file')
        with open(self.out_bursty_entities().path,'w',encoding='utf-8') as file_out:
            file_out.write('\n'.join(bursty_entities))

        with open(self.out_stats().path,'w',encoding='utf-8') as file_out:
            file_out.write('\n'.join(term_burstiness_stats))
