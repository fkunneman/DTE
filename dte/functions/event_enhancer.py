
from dte.classes.event import Event

class EventEnhancer:

    def __init__(self):
        self.events = []

    def set_events(self,events):
        self.events = events

    def return_events(self):
        return self.events

    def enhance(self,check_status=True):
        for i,event in enumerate(self.events):
            if not (check_status and event.status == 'stable'):
                event.resolve_overlap_entities()
                event.order_entities()
                event.rank_tweets()
                event.set_event_location()
                event.status = 'stable'

