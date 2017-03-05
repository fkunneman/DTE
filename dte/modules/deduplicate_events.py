
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_deduplicator
from dte.classes import event

################################################################################
###Event deduplicator
################################################################################

@registercomponent
class DeduplicateEvents(StandardWorkflowComponent):

    similarity_threshold = Parameter(default = 0.7)

    def accepts(self):
        return InputFormat(self, format_id='events', extension='.events.json')

    def autosetup(self):
        return DeduplicateEventsTask

class DeduplicateEventsTask(Task):

    in_events = InputSlot()

    similarity_threshold = Parameter()

    def out_deduplicated_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.json', addextension='.deduplicated.json')

    def run(self):

        # read in events
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            event_objs.append(eventobj)

        # initialize event deduplicator
        similarity_threshold = float(self.similarity_threshold)
        print('Deduplicating; number of events at start:',len(event_objs))
        deduplicator = event_deduplicator.EventDeduplicator()
        deduplicator.set_events(event_objs)
        deduplicator.deduplicate_events(similarity_threshold)
        deduplicated_events = deduplicator.return_events()
        print('Done. number of events after deduplication:',len(deduplicated_events))        

        # write deduplicated
        out_deduplicated_events = [event.return_dict() for event in deduplicated_events]
        with open(self.out_deduplicated_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_deduplicated_events,file_out)
