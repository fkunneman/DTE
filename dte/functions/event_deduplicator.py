
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

    def is_similar(self,index1,index2,similarity_threshold):
        similarity = self.event_similarity[index[1],index[2]]
        similar = True if similarity > similarity_threshold else False
        return similar

    def deduplicate_events(self,similarity_threshold):
        self.set_index_event()
        self.set_event_similarity()
        # sort by date
        dates = sorted(list(set([event.datetime.date() for event in self.events])))
        new_events = []
        for date in dates:
            print(date)
            candidates = self.return_events_date(date)
            print('Event entities before merge:',[x[1].entities for x in candidates])
            merged = [candidates[0]]
            for index2, event2 in candidates[1:]:
                similar = False
                for index1, event1 in merged:
                    if self.is_similar(index1,index2,similarity_threshold):
                        event1.merge(event2)
                        similar = True
                        break
                if not similar:
                    merged.append([index2,event2])
            new_events.extend([x[1] for x in merged])
            print('Event entities after merge:',[x[1].entities for x in merged])
        self.events = new_events

    def set_index_event(self):
        self.index_event = {}
        for i,event in enumerate(self.events):
            self.index_event[i] = event

    def set_event_similarity(self):
        tfidf_vectorizer = TfidfVectorizer()
        big_documents = tfidf_vectorizer.fit_transform([self.get_concatenated_tweets_event(self.index_event[i]) for i in range(len(self.events))])
        self.event_similarity = cosine_similarity(big_documents,big_documents)

    def get_concatenated_tweets_event(self,event):
        return ' '.join([tweet.text for tweet in event.tweets])

    def return_events_date(self,date):
        return [[i,self.index_events[i] for i in range(len(events)) if self.index_event[i].datetime.date() == date]]
