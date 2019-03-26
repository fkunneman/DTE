
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
class FilterEvents(StandardWorkflowComponent):

    citylist = Parameter()
    overlap_threshold = Parameter(default = 0.2)
    similarity_threshold = Parameter(default = 0.7)

    def accepts(self):
        return (
            InputFormat(self, format_id='merged_events', extension='.merged'),
            InputComponent(self, MergeEvents, overlap_threshold=self.overlap_threshold,similarity_threshold=self.similarity_threshold)
        )

    def autosetup(self):
        return FilterEventsTask

class FilterEventsTask(Task):

    in_merged_events = InputSlot()

    citylist = Parameter()

    def out_filtered_events(self):
        return self.outputfrominput(inputformat='merged_events', stripextension='.merged', addextension='.filtered')

    def run(self):

        # read in events
        print('Reading in events')
        with open(self.in_merged_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed,txt=False)
            event_objs.append(eventobj)

        print('Reading in citylist')
        # read in citylist
        with open(self.citylist,'r',encoding='utf-8') as file_in:
            citylist = [line.strip() for line in file_in.read().strip().split('\n')]

        # initialize event filter
        print('Filtering; number of events at start:',len(event_objs))
        filter = event_filter.EventFilter()
        filter.add_events(event_objs)
        filter.apply_filter(citylist)
        events_filtered = filter.return_events()
        print('Done. number of events after filter:',len(events_filtered))        

        # write filter
        out_filtered_events = [event.return_dict(txt=False) for event in events_filtered]
        with open(self.out_filtered_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_filtered_events,file_out)
