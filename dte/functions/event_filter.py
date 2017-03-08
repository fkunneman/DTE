
import re

from dte.functions import dutch_timex_extractor, cityref_extractor

class EventFilter:

    def __init__(self):
        self.events = []
        self.citylist = []

    def set_citylist(self,citylist):
        self.citylist = citylist

    def add_events(self,events):
        self.events.extend(events)

    def return_events(self):
        return self.events

    def strip_cities(self,event):
        ce = cityref_extractor.CityrefExtractor(self.citylist)
        ce.find_cityrefs(' '.join([entity.replace('#','') for entity in event.entities]))
        event.entities = list(set(event.entities) - set([entity.lower() for entity in ce.return_cityrefs()]))

    def strip_time(self,event):
        dte = dutch_timex_extractor.Dutch_timex_extractor(' '.join(event.entities), event.datetime)
        dte.extract_refdates()
        event.entities = list(set(event.entities) - set([rd[0] for rd in dte.refdates]))

    def strip_int(self,event):
        event.entities = [entity for entity in event.entities if not re.match(r'^\d+-?$',entity)]

    def strip_event(self,event):
        self.strip_cities(event)
        self.strip_time(event)
        self.strip_int(event)

    def apply_filter(self,citylist):
        self.set_citylist(citylist)
        filtered_events = []
        for event in self.events:
            fs = event.entities
            self.strip_event(event)
            fe = event.entities
            if not len(event.entities) == 0:
                filtered_events.append(event)
        self.events = filtered_events
