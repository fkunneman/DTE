
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import event_filter
from dte.classes import event, tweet

#########################################
### All events
#########################################

@registercomponent
class ArchiveEvents(WorkflowComponent):

    events = Parameter()
    
    def accepts(self):
        return [ ( InputFormat(self,format_id='events',extension='.events',inputparameter='events') ) ]

    def setup(self, workflow, input_feeds):

        event_archiver = workflow.new_task('archive_events', ArchiveEventsTask, autopass=False)
        event_archiver.in_events = input_feeds['events']

        return event_archiver

class ArchiveEventsTask(Task):

    in_events = InputSlot()

    def out_archivedir(self):
        return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.archive')

    def out_active_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.active.events')

    def run(self):

        # initiate directory 
        self.setup_output_dir(self.out_archivedir().path)

        # read events
        datebound = datetime.now() - datetime.timedelta(days=100)
        date_events = defaultdict(list)
        active_events = []
        print('Reading events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
            for i,ed in enumerate(eventdicts):
                eventobj = event.Event()
                eventobj.import_eventdict(ed)
                if eventobj.datetime < datebound:
                    date_events[''.join(str(eventobj.datetime).split()[0].split('-'))].append(eventobj)
                else:
                    active_events.append(eventobj)

        # write archives
        print('Writing archives')
        for date in sorted(list(date_events.keys())):
            print(date)
            events = date_events[date]
            out_events = [event.return_dict(txt=False) for event in events]
            outfile = self.out_archivedir().path + '/events_' + date + '.json'
            with open(outfile,'w',encoding='utf-8') as file_out:
                json.dump(out_events,file_out)

        # write active events
        print('Writing active events')
        out_active_events = [event.return_dict(txt=False) for event in active_events]
        with open(self.out_active_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_active_events,file_out)

