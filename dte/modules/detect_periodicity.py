
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import glob
import os
import datetime 
from collections import defaultdict

from dte.functions import periodicity_detector
from dte.classes import event


################################################################################
###Periodicity detector
################################################################################

@registercomponent
class DetectPeriodicity(StandardWorkflowComponent):

    periodicity_threshold = Parameter(default = 0.6)

    def accepts(self):
        return InputFormat(self, format_id='events', extension='.lecl.events')

    def autosetup(self):
        return DetectPeriodicityTask

class DetectPeriodicityTask(Task):

    in_events = InputSlot()

    periodicity_threshold = Parameter()

    def out_periodic_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.lecl.events', addextension='.periodic.events.enhanced')

    def run(self):

        # read in current events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            event_objs.append(eventobj)

        # initialize periodicity detector
        pd = periodicity_detector.PeriodicityDetector()
        pd.set_events(event_objs)

        # apply periodicity detection
        print('Applying periodicity detection')
        pd.main(float(self.periodicity_threshold))

        # write new events
        print('Done. Writing to file')
        new_events = pd.return_events()
        out_events = [event.return_dict() for event in new_events]
        with open(self.out_periodic_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_events,file_out)

################################################################################
###Periodicity extender
################################################################################

@registercomponent
class ExtendPeriodicity(StandardWorkflowComponent):

    periodicity_threshold = Parameter(default = 0.6)

    def accepts(self):
        return InputFormat(self, format_id='events', extension='.lecl.events')

    def autosetup(self):
        return ExtendPeriodicityTask

class ExtendPeriodicityTask(Task):

    in_events = InputSlot()

    periodicity_threshold = Parameter()

    def out_periodic_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.lecl.events', addextension='.periodic.events.enhanced')

    def run(self):

        # read in current events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            event_objs.append(eventobj)

        # initialize periodicity detector
        pd = periodicity_detector.PeriodicityDetector()
        pd.set_events(event_objs)

        # apply periodicity detection
        print('Applying periodicity detection')
        pd.main(float(self.periodicity_threshold))

        # write new events
        print('Done. Writing to file')
        new_events = pd.return_events()
        out_events = [event.return_dict() for event in new_events]
        with open(self.out_periodic_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_events,file_out)

################################################################################
###Periodicity applyer
################################################################################

@registercomponent
class DetectPeriodicity(StandardWorkflowComponent):

    periodicity_threshold = Parameter(default = 0.6)

    def accepts(self):
        return InputFormat(self, format_id='events', extension='.lecl.events')

    def autosetup(self):
        return DetectPeriodicityTask

class DetectPeriodicityTask(Task):

    in_events = InputSlot()

    periodicity_threshold = Parameter()

    def out_periodic_events(self):
        return self.outputfrominput(inputformat='events', stripextension='.lecl.events', addextension='.lecl.periodic.events')

    def out_periodic_patterns(self):
        return self.outputfrominput(inputformat='events', stripextension='.lecl.events', addextension='.periodic_patterns')

    def run(self):

        # read in current events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())
        event_objs = []
        for ed in eventdicts:
            eventobj = event.Event()
            eventobj.import_eventdict(ed)
            event_objs.append(eventobj)

        # initialize periodicity detector
        pd = periodicity_detector.PeriodicityDetector()
        pd.set_events(event_objs)

        # apply periodicity detection
        print('Applying periodicity detection')
        pd.main(float(self.periodicity_threshold))

        # write new events
        print('Done. Writing to file')
        new_events = pd.return_events()
        out_events = [event.return_dict() for event in new_events]
        with open(self.out_periodic_events().path,'w',encoding='utf-8') as file_out:
            json.dump(out_events,file_out)
