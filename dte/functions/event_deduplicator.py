
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
        similarity = e_similarity[index1,index2]
        similar = True if similarity > similarity_threshold else False
        return similar

    def deduplicate_events(self,similarity_threshold):
        # set big documents
        print('Fitting tfidf')
        self.set_tfidf()
        # sort by date
        dates = sorted(list(set([event.datetime.date() for event in self.events])))
        new_events = []
        for date in dates:
            print('Date:',date)
            candidates = self.return_events_date(date)
            index_candidates = [[i,c] for i,c in enumerate(candidates)]
            event_similarity = self.set_event_similarity(candidates)
            print('Event entities before merge:','   ---   '.join([', '.join(x.entities) for x in candidates]).encode('utf-8'))
            merged = [index_candidates[0]]
            for index2, event2 in index_candidates[1:]:
                similar = False
                for index1, event1 in merged:
                    if self.is_similar(index1,index2,event_similarity,similarity_threshold):
                        event1.merge(event2)
                        similar = True
                        break
                if not similar:
                    merged.append([index2,event2])
            print('AFTER','   ---   '.join([', '.join(x[1].entities) for x in merged]).encode('utf-8'))
            new_events.extend([x[1] for x in merged])
        self.events = new_events

    def set_tfidf(self):
        self.tfidf_vectorizer = TfidfVectorizer()
        self.tfidf_vectorizer.fit([self.get_concatenated_tweets_event(event) for event in self.events])
        
    def set_event_similarity(self,events):
        big_docs = self.tfidf_vectorizer.transform([self.get_concatenated_tweets_event(event) for event in events])
        event_similarity = cosine_similarity(big_docs,big_docs)
        return event_similarity

    def get_concatenated_tweets_event(self,event):
        return ' '.join([tweet.text for tweet in event.tweets])

    def return_events_date(self,date):
        return [event for event in self.events if event.datetime.date() == date]
