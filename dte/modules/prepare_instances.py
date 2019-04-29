
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
class PrepareInstances(StandardWorkflowComponent):

    def accepts(self):
        return [ ( InputFormat(self,format_id='events',extension='.events',inputparameter='events') ) ]

    def autosetup(self):
        return PrepareInstancesTask

class PrepareInstancesTask(Task):

    in_events = InputSlot()

    def out_instances(self):
        return self.outputfrominput(inputformat='events', stripextension='.events', addextension='.events.instances')

    def run(self):

        # initiate directory with instances
        self.setup_output_dir(self.out_instances().path)

        # read in events
        print('Reading in events')
        with open(self.in_events().path, 'r', encoding = 'utf-8') as file_in:
            eventdicts = json.loads(file_in.read())

        # extract information
        print('Extracting text')
        ids = []
        txt = []
        for ed in eventdicts:
            ids.append(ed['mongo_id'])
            tweetstxt = []
            for tweettext in [' '.join([tweet['user'],tweet['text']]) for tweet in ed['tweets']] + [' '.join([tweet['user'],tweet['text']]) for tweet in ed['tweets_added']]
                print('BEFORE',tweettext.encode('utf-8'))
                tokens = tweettext.split()
                for i,token in enumerate(tokens):
                    if token[:4] == 'http':
                        tokens[i] = 'THISISATWITTERLINK'
                tweettext_new = ' '.join(tokens).replace('\n',' ').replace('\r',' ')
                print('AFTER',tweettext_new.encode('utf-8'))
                tweetstxt.append(tweettext_new)
            txt.append(' '.join(tweetstxt))
            quit()

        # write data
        print('Done. Writing to files')
        with open(self.out_instances().path + '/events_meta.txt','w',encoding='utf-8') as out:
            out.write('\n'.join(ids))

        with open(self.out_instances().path + '/events_text.txt','w',encoding='utf-8') as out:
            out.write('\n'.join(txt))
