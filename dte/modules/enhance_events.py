
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_enhancer
from dte.classes import event
from dte.modules.deduplicate_events import DeduplicateEvents

################################################################################
###Event enhancer
################################################################################

@registercomponent
class EnhanceEvents(StandardWorkflowComponent):

    similarity_threshold = Parameter(default = 0.7)
    
    def accepts(self):
        return (
            InputFormat(self, format_id='events', extension='.deduplicated'),
            InputComponent(self, DeduplicateEvents, similarity_threshold=self.similarity_threshold)
        )

    def autosetup(self):
        return EnhanceEventsTask

class EnhanceEventsTask(Task):

    in_deduplicated_events = InputSlot()

    def out_enhanced_events(self):
        return self.outputfrominput(inputformat='deduplicated_events', stripextension='.deduplicated', addextension='.enhanced')

    def run(self):

        # read in events
        print('Reading in events')
        with open(self.in_deduplicated_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            event_objs.append(eventobj)

        # initialize event enhancer
        print('Enhancing events')
        enhancer = event_enhancer.EventEnhancer()
        enhancer.set_events(event_objs)
        enhancer.enhance()
        enhanced_events = enhancer.return_events()     

        # write deduplicated
        out_enhanced_events = [event.return_dict() for event in enhanced_events]
        with open(self.out_enhanced_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_enhanced_events,file_out)
