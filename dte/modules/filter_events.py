
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_filter
from dte.classes import event

@registercomponent
class FilterEvents(WorkflowComponent):

    citylist = Parameter()

    def accepts(self):
        return InputFormat(self, format_id='events', extension='.merged')

    def autosetup(self):
        return MergeEventsTask

class MergeEventsTask(Task):

    in_events = InputSlot()

    citylist = Parameter()

    def out_filtered_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.merged', addextension='.filtered')

    def run(self):

        # read in events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
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
        with open(self.out_filtered_events().path,'w',encoding='utf-8') as file_out:
            json.dump(events_filtered,file_out)
