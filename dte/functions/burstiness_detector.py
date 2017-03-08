
from collections import defaultdict
import numpy

class BurstinessDetector:

    def __init__(self):
        self.events =  []
        self.entity_counts = defaultdict(list)
        self.entity_dates = defaultdict(list)
        self.entity_datecount = defaultdict(lambda : {})
        self.entity_burstiness = []

    def return_entity_burstiness(self):
        sorted_entity_burstiness = sorted(self.entity_burstiness, key=lambda k : k[3],reverse=True) 
        return sorted_entity_burstiness

    def set_entity_counts(self,entity_counts):
        self.entity_counts = entity_counts

    def set_entity_datecount(self,entity_datecount):
        self.entity_datecount = entity_datecount

    def set_events(self,events):
        self.events = events

    def set_entities_dates(self):
        for event in self.events:
            for entity in event.entities:
                self.entity_dates[entity].append(event.datetime.date())
        for entity in self.entity_dates.keys():
            self.entity_dates[entity] = list(set(self.entity_dates[entity]))

    def calculate_burstiness(self):
        # for each entity
        for entity in self.entity_counts.keys():
            burstiness_scores = []
            # calculate avg frequency
            avg_frequency = self.calculate_average_frequency(self.entity_counts[entity])
            # for each date at which the entity represents an event
            for date in self.entity_dates[entity]:
                # calculate burstiness at this date
                try:
                    burstiness_scores.append([self.entity_datecount[entity][date],self.calculate_date_burstiness(avg_frequency,self.entity_datecount[entity][date])])
                except: # not frequencies available for date
                    continue
            # summarize burstiness
            if len(burstiness_scores) > 0:
                summary = self.summarize_burstiness([bs[1] for bs in burstiness_scores])
                self.entity_burstiness.append([entity,burstiness_scores,avg_frequency,summary[0],summary[1]])

    def calculate_average_frequency(self,counts):
        return numpy.mean(counts)

    def calculate_date_burstiness(self,avg,datecount):
        return datecount / avg

    def summarize_burstiness(self,scores):
        return [numpy.mean(scores), numpy.std(scores)]
