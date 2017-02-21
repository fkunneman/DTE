

from luiginlp.engine import Task, StandardWorkflowComponent, InputFormat, InputComponent, registercomponent, InputSlot, Parameter, BoolParameter

import json

from functions import entity_extractor, helpers
from classes import tweet, commonness

class AggregateTweetsTask(Task):

    in_cityref = InputSlot()

    commonness_txt = Parameter()
    commonness_cls = Parameter()
    commonness_corpus = Parameter()
    ngrams_score = Parameter()

    def out_entity(self):
        return self.outputfrominput(inputformat='cityref', stripextension='.json', addextension='.entity.json')

    def run(self):

        # set commonness object
        print('setting commonness object', commonness_txt, commonness_cls, commonness_corpus, ngrams_score)
        cs = commonness.Commonness()
        cs.set_classencoder(self.commonness_txt, self.commonness_cls, self.commonness_corpus)
        cs.set_dmodel(self.ngrams_score)
        
        # read in tweets
        with open(self.in_location().path, 'r', encoding = 'utf-8') as file_in:
            tweetdicts = json.loads(file_in.read())

        # format as tweet objects
        tweets = []
        for td in tweetdicts:
            tweetobj = tweet.Tweet()
            tweetobj.import_tweetdict(td)
            tweets.append(tweetobj)

        # extract entities
        for tweetobj in tweets:
            # remove already extracted time and locations from the tweet, forming it into chunks
            datestrings = tweetobj.string_refdates
            cities = tweetobj.string_cityrefs
            tweet_chunks = helpers.remove_pattern_from_string(tweetobj.text,datestrings+cities)
            # find entities in every chunk
            ee = entity_extractor.EntityExtractor()
            for chunk in tweet_chunks:
                tokens = chunk.split()
                ee.extract_entities(tokens)
                ee.filter_entities_threshold()
            tweetobj.set_entities(ee.entities)

        # write to file
        outtweets = [tweet.return_dict() for tweet in tweets]
        with open(self.out_entity().path,'w',encoding='utf-8') as file_out:
            json.dump(outtweets,file_out)
        
@registercomponent
class ExtractEntities(StandardWorkflowComponent):

    commonness_txt = Parameter()
    commonness_cls = Parameter()
    commonness_corpus = Parameter()
    ngrams_score = Parameter()    

    def accepts(self):
        return InputFormat(self, format_id='cityref', extension='.json')
                    
    def autosetup(self):
        return ExtractEntitiesTask
