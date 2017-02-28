
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from dte.classes.event import Event
from dte.functions import helpers

class EventDeduplicator:

    def __init__(self):
        self.events = []

    def set_events(self,events):
        self.events = events

    def add_events(self,events):
        self.events.extend(events)

    def return_events(self):
        return self.events

    def is_similar(self,index1,index2,e_similarity,similarity_threshold):
        similarity = e_similarity[index[1],index[2]]
        similar = True if similarity > similarity_threshold else False
        return similar

    def deduplicate_events(self,similarity_threshold):
        # sort by date
        dates = sorted(list(set([event.datetime.date() for event in self.events])))
        new_events = []
        for date in dates:
            candidates = self.return_events_date(date)
            index_events = [[i,event] for i,event in enumerate(candidates)]
            event_similarity = self.set_event_similarity(index_events)
            print('Event entities before merge:',[x.entities for x in candidates])
            merged = [index_events[0]]
            for index2, event2 in index_events[1:]:
                similar = False
                for index1, event1 in merged:
                    if self.is_similar(index1,index2,event_similarity,similarity_threshold):
                        event1.merge(event2)
                        similar = True
                        break
                if not similar:
                    merged.append([index2,event2])
            print('AFTER',[x[1].entities for x in merged])
            new_events.extend([x[1] for x in merged])
            print('Event entities after merge:',[x[1].entities for x in merged])
        self.events = new_events

    def set_event_similarity(self,index_events):
        tfidf_vectorizer = TfidfVectorizer()
        big_documents = tfidf_vectorizer.fit_transform([self.get_concatenated_tweets_event(ie[1]) for ie in index_events])
        event_similarity = cosine_similarity(big_documents,big_documents)
        return event_similarity

    def get_concatenated_tweets_event(self,event):
        return ' '.join([tweet.text for tweet in event.tweets])

    def return_events_date(self,date):
        return [[i,self.index_events[i]] for i in range(len(events)) if self.index_event[i].datetime.date() == date]
