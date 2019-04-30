
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_filter
from dte.classes import event
from dte.modules.merge_events import MergeEvents

@registercomponent
class UpdateEventTypes(WorkflowComponent):

    events = Parameter()
    predictiondir = Parameter()

    text = BoolParameter()

    def accepts(self):
        return [ ( InputFormat(self,format_id='predictiondir',extension='.instances',inputparameter='predictiondir'), InputFormat(self,format_id='events',extension='.events.integrated',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        event_type_updater = workflow.new_task('update_event_types', UpdateEventTypesTask, autopass=True, text=self.text)
        event_type_updater.in_events = input_feeds['events']
        event_type_updater.in_predictiondir = input_feeds['predictiondir']

        return event_type_updater

class UpdateEventTypesTask(Task):

    in_events = InputSlot()
    in_predictiondir = InputSlot()

    text = BoolParameter()

    def out_updated_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.events.integrated', addextension='.types.events.integrated')

    def run(self):

    # read prediction data
    with open(self.in_predictiondir().path + '/events_meta.txt','r',encoding='utf=8') as file_in:
        meta = file_in.read().strip().split('\n')

    with open(self.in_predictiondir().path + '/events_text.predictions.txt','r',encoding='utf=8') as file_in:
        predictions = file_in.read().strip().split('\n')

    with open(self.in_predictiondir().path + '/events_text.full_predictions.txt','r',encoding='utf=8') as file_in:
        lines = file_in.read().strip().split('\n')
    label_order = lines[0].split('\t')
    full_predictions = [line.split('\t') for line in lines[1:]]

    # read in events
    print('Reading in events')
    with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
        eventdicts = json.loads(file_in.read())

    # index events
    id_event = {}
    for ed in eventdicts:
        id_event[ed['mongo_id']] = ed

    # for each prediction
    for i,mid in enumerate(meta):
        prediction = predictions[i]
        prediction_score = dict(zip(label_order,full_predictions[i]))
        ed = id_event[mid]
        ed['eventtype'] = prediction
        ed['eventtype_scores'] = prediction_score

    # write output
    out_updated_events = [event.return_dict(txt=self.text) for event in eventdicts]
    with open(self.out_updated_events().path,'w',encoding='utf-8') as file_out:
        json.dump(out_updated_events,file_out)
