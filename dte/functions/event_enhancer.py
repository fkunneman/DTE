
from dte.classes.event import Event

class EventEnhancer:

    def __init__(self):
        self.events = []

    def set_events(self,events):
        self.events = events

    def return_events(self):
        return self.events

    def enhance(self):
        for event in self.events:
            event.resolve_overlap_entities()
            event.order_entities()
            event.rank_tweets()
    