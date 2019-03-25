
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import gzip
import io
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

    entity_counts = Parameter()
    dates = Parameter()
    vocabulary = Parameter()
    events = Parameter()

    burstiness_threshold = IntParameter()

    def accepts(self):
        return [ ( InputFormat(self,format_id='entity_counts',extension='.counts.npz',inputparameter='entity_counts'), InputFormat(self,format_id='dates',extension='.counts_dates',inputparameter='dates'), InputFormat(self,format_id='vocabulary',extension='.counts_vocabulary',inputparameter='vocabulary'), InputFormat(self,format_id='events',extension='.events',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        burstiness_assessor = workflow.new_task('assess_burstiness', AssessBurstinessTask, autopass=False, burstiness_threshold=self.burstiness_threshold)
        burstiness_assessor.in_entity_counts = input_feeds['entity_counts']
        burstiness_assessor.in_dates = input_feeds['dates']
        burstiness_assessor.in_vocabulary = input_feeds['vocabulary']
        burstiness_assessor.in_events = input_feeds['events']

        return burstiness_assessor

class AssessBurstinessTask(Task):

    in_entity_counts = InputSlot()
    in_dates = InputSlot()
    in_vocabulary = InputSlot()
    in_events = InputSlot()

    burstiness_threshold = Parameter()

    def out_stats(self):
        return self.outputfrominput(inputformat='entity_counts', stripextension='.counts.npz', addextension='.burstiness_stats.txt')

    def out_bursty_entities(self):
        return self.outputfrominput(inputformat='entity_counts', stripextension='.counts.npz', addextension='.burstiness.txt')

    def run(self):

        # read in entity counts
        print('Reading countfiles')
        loader = numpy.load(self.in_entity_counts().path)
        counts = sparse.csr_matrix((loader['data'], loader['indices'], loader['indptr']), shape = loader['shape'])
        with open(self.in_vocabulary().path,'r',encoding='utf-8') as file_in:
            vocabulary = file_in.read().strip().split('\n')
        with open(self.in_dates().path,'r',encoding='utf-8') as file_in:
            dates = file_in.read().strip().split('\n')
        print('Num terms:',len(vocabulary),'Num dates:',len(dates),'Counts shape:',counts.shape)

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
        cursor = range(0,counts.shape[0],1000)
        terms = []
        term_burstiness = []
        term_burstiness_stats = []
        for i,term in enumerate(vocabulary):
            if i in cursor:
                print('Term',i,'of',counts.shape[0])
            burstiness = []
            burstiness_stats = []
            for date in entity_dates[term]:
                try:
                    index = dates.index(date)
                    min_index = index - 30 if (index-30 >= 0) else 0
                    max_index = index + 30 if (index+30 <= counts.shape[1]) else counts.shape[1]
                    if max_index - min_index > 40:
                        # if term occurs more often than 10 times
                        if counts[i,min_index:max_index].sum() > 10:
                            burstiness.append(counts[i,index] / counts[i,min_index:max_index].mean())
                            burstiness_stats.append([counts[i,index],counts[i,min_index:max_index].mean()])
                except:
                    continue
            if len(burstiness) > 0:
                avg_burstiness = numpy.mean(burstiness)
                if avg_burstiness > float(self.burstiness_threshold):
                    term_burstiness.append(1)
                else:
                    term_burstiness.append(0)
                term_burstiness_stats.append('****'.join([term,str(avg_burstiness),'-!-'.join(['-'.join([str(x) for x in y]) for y in burstiness_stats])])) 
            else:
                term_burstiness.append(0)
                term_burstiness_stats.append('Frequency too low')
        bursty_entities = numpy.array(vocabulary)[numpy.array(term_burstiness).nonzero()].tolist()

        # write burstiness
        print('Done. Writing to file')
        with open(self.out_bursty_entities().path,'w',encoding='utf-8') as file_out:
            file_out.write('\n'.join(bursty_entities))

        with open(self.out_stats().path,'w',encoding='utf-8') as file_out:
            file_out.write('\n'.join(term_burstiness_stats))
