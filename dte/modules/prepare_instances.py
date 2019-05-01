
from luiginlp.engine import Task, StandardWorkflowComponent, WorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, IntParameter, BoolParameter

import json
import re

@registercomponent
class PrepareInstances(StandardWorkflowComponent):

    def accepts(self):
        return InputFormat(self,format_id='events',extension='.events.integrated')

    def autosetup(self):
        return PrepareInstancesTask

class PrepareInstancesTask(Task):

    in_events = InputSlot()

    def out_instances(self):
        return self.outputfrominput(inputformat='events', stripextension='.events.integrated', addextension='.events.instances')

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
        counter = list(range(0,len(eventdicts),1000))
        for i,ed in enumerate(eventdicts):
            if i in counter:
                print('Event',i,'of',len(eventdicts))
            tweetstxt = []
            for tweettext in [' '.join([tweet['user'],tweet['text']]) for tweet in ed['tweets']] + [' '.join([tweet['user'],tweet['text']]) for tweet in ed['tweets_added']]:
                if re.search('http',tweettext):
                    tokens = tweettext.split()
                    for j,token in enumerate(tokens):
                        if token[:4] == 'http':
                            tokens[j] = 'THISISATWITTERLINK'
                    tweetstxt.append(' '.join(tokens).replace('\n',' ').replace('\r',' '))
                else:
                    tweetstxt.append(tweettext.replace('\n',' ').replace('\r',' '))                
            if ' '.join(tweetstxt).strip() == '':
                continue
            else:
                ids.append(ed['mongo_id'])
                txt.append(' '.join(tweetstxt))
 
        # write data
        print('Done. Writing to files')
        with open(self.out_instances().path + '/events_meta.txt','w',encoding='utf-8') as out:
            out.write('\n'.join(ids))

        with open(self.out_instances().path + '/events_text.txt','w',encoding='utf-8') as out:
            out.write('\n'.join(txt))
