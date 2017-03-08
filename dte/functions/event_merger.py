
class EventMerger:

    def __init__(self):
        self.events = []

    def add_events(self,events):
        self.events.extend(events)

    def return_events(self):
        return self.events

    def find_merge(self,event,overlap_threshold):
        candidates = self.return_events_date(event.datetime.date())
        overlap = False
        for candidate in candidates:
            if self.has_overlap(candidate,event,overlap_threshold):
                candidate.merge(event)
                overlap = True
                break
        if not overlap:
            self.events.append(event)

    def find_merges(self,overlap_threshold):
        # sort by date
        dates = sorted(list(set([event.datetime.date() for event in self.events])))
        new_events = []
        for date in dates:
            candidates = self.return_events_date(date)
            merged = [candidates[0]]
            for event2 in candidates[1:]:
                overlap = False
                for event1 in merged:
                    if self.has_overlap(event1,event2,overlap_threshold):
                        event1.merge(event2)
                        overlap = True
                        break
                if not overlap:
                    merged.append(event2)
            new_events.extend(merged)
        self.events = new_events

    def has_overlap(self,event1,event2,overlap_threshold):
        intersect = list(set([tweet.id for tweet in event1.tweets]) & set([tweet.id for tweet in event2.tweets]))
        overlap_percent = len(intersect) / len(event1.tweets)
        overlap = True if overlap_percent > overlap_threshold else False
        overlap = True if set(event1.entities) == set(event2.entities)
        return overlap

    def return_events_date(self,date):
        return [event for event in self.events if event.datetime.date() == date]
