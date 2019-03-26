
from luiginlp.engine import Task, StandardWorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, BoolParameter

import json

@registercomponent
class IndexAllTweets(StandardWorkflowComponent):

    def accepts(self):
        return (
            InputFormat(self, format_id='tweetdir', extension='.tweets'),
        )
                    
    def autosetup(self):
        return IndexAllTweetsTask

class IndexAllTweetsTask(Task):

    in_tweetdir = InputSlot()

    def out_indexed_tweets(self):
        return self.outputfrominput(inputformat='tweetdir', stripextension='.tweets', addextension='.tweets_indexed.json')

    def run(self):
        
        # read in tweets
        indexed_tweets = {}
        tweetsubdirs = sorted([ subdir for subdir in glob.glob(self.in_tweetdir().path + '/*') ])
        for tweetsubdir in tweetsubdirs:
            print(tweetsubdir)
            # go through all tweet files
            tweetfiles = [ tweetfile for tweetfile in glob.glob(tweetsubdir + '/*.entity.json') ]
            for tweetfile in tweetfiles:
                tweetfilestr = '/'.join(tweetfile.split('/')[-2:])
                # read in tweets
                with open(tweetfile, 'r', encoding = 'utf-8') as file_in:
                    tweetdicts = json.loads(file_in.read())
                for i,td in enumerate(tweetdicts):
                    indexed_tweets[td['id']] = [tweetfilestr,i]

        # write to file
        with open(self.out_indexed_tweets().path,'w',encoding='utf-8') as file_out:
            json.dump(indexed_tweets,file_out)
        



