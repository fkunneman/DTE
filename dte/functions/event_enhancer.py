
from dte.classes.event import Event

class EventEnhancer:

    def __init__(self):
        self.events = []

    def set_events(self,events):
        self.events = events

    def return_events(self):
        return self.events

    def enhance(self):
        l = len(self.events)
        shows = range(10000, l, 10000) #to write intermediate output
        checks = range(0, l, 1000)
        new_events = []
        for i,event in enumerate(self.events):
            if i in checks:
                print('event',i,'of',l)
            
            event.resolve_overlap_entities()
            event.order_entities()
            event.rank_tweets()
            event.set_event_location()

