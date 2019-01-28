
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import burstiness_detector
from dte.classes import event


################################################################################
###Periodicity detector
################################################################################

@registercomponent
class AssessBurstiness(WorkflowComponent):

    entity_counts = Parameter()
    events = Parameter()

    date = Parameter() # to define the output file

    def accepts(self):
        return [ ( InputFormat(self,format_id='entity_counts',extension='.entity_counts',inputparameter='entity_counts'), InputFormat(self,format_id='events',extension='.integrated',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        burstiness_assessor = workflow.new_task('assess_burstiness', AssessBurstinessTask, autopass=False, date=self.date)
        burstiness_assessor.in_entity_counts = input_feeds['entity_counts']
        burstiness_assessor.in_events = input_feeds['events']

        return burstiness_assessor

class AssessBurstinessTask(Task):

    in_entity_counts = InputSlot()
    in_events = InputSlot()
    
    date = Parameter()

    def out_entity_burstiness(self):
        return self.outputfrominput(inputformat='entity_counts', stripextension='.entity_counts', addextension='.entity_counts.' + self.date + '.burstiness.json')

    def run(self):

        # read in entity counts
        print('Reading in countfiles')
        countfiles = sorted([countfile for countfile in glob.glob(self.in_entity_counts().path + '/*')])
        countdicts = []
        for countfile in countfiles:
            dateinfo = countfile.split('/')[-1]
            date = datetime.date(int(dateinfo[:4]),int(dateinfo[4:6]),int(dateinfo[6:8]))
            with open(countfile,'r',encoding='utf-8') as file_in:
                countdicts.append([date,json.loads(file_in.read())])
        # extract entities
        print('Extracting entities')
        entities = set(sum([list(cd[1].keys()) for cd in countdicts],[]))
        # extract counts per entity
        print('Extracting counts per entity')
        counts = defaultdict(list)
        date_counts = defaultdict(lambda : {})
        for countdict in countdicts:
            date = countdict[0]
            cd = countdict[1]
            for entity in list(entities - set(cd.keys())):
                counts[entity].append(0)
                date_counts[entity][date] = 0
            for entity in cd.keys():
                counts[entity].append(int(cd[entity]))
                date_counts[entity][date] = cd[entity]

        # read in current events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            event_objs.append(eventobj)

        # start burstiness detection
        print('Initializing burstiness detection')
        bd = burstiness_detector.BurstinessDetector()
        bd.set_entity_counts(counts)
        bd.set_entity_datecount(date_counts)
        bd.set_events(event_objs)
        print('Extracting event dates per entity')
        bd.set_entities_dates()
        print('Calculating burstiness for each entity event')
        bd.calculate_burstiness()
        burstiness = bd.return_entity_burstiness()

        # write burstiness
        print('Done. Writing to file')
        with open(self.out_entity_burstiness().path,'w',encoding='utf-8') as file_out:
            file_out.write('\n'.join(['\t'.join([str(x) for x in line]) for line in burstiness]))
