
from collections import defaultdict
import numpy

class BurstinessDetector:

    def __init__(self):
        self.events =  []
        self.entity_counts = defaultdict(list)
        self.entity_dates = defaultdict(list)
        self.entity_datecount = defaultdict()
        self.entity_burstiness = {}

    def return_entity_burstiness(self):
        return entity_burstiness

    def set_entity_counts(self,entity_counts):
        self.entity_counts = entity_counts

    def set_entity_datecount(self,entity_datecount):
        self.entity_datecount = entity_datecount

    def set_events(self,events):
        self.events = events

    def set_entities_dates(self):
        for event in self.events:
            for entity in event.entities:
                self.entity_dates[entity] = event.datetime.date()

    def calculate_burstiness(self):
        # for each entity
        for entity in self.entity_counts.keys():
            burstiness_scores = []
            # calculate avg frequency
            avg_frequency = self.calculate_average_frequency(self.entity_counts[entity])
            # for each date at which the entity represents an event
            for date in entity_dates:
                # calculate burstiness at this date
                burstiness_scores.append(self.calculate_date_burstiness(avg_frequency,self.entity_datecount[entity][date]))
            # summarize burstiness
            summary = self.summarize_burstiness(burstiness_scores)
            self.entity_burstiness = {'burstiness_scores':burstiness_scores,'average_frequency':avg_frequency,'average_burstiness':summary[0],'burstiness_stdev':summary[1]}

    def calculate_average_frequency(self,counts):
        return numpy.mean(counts)

    def calculate_date_burstiness(self,avg,datecount):
        return datecount / avg

    def summarize_burstiness(self,scores):
        return [numpy.mean(scores), numpy.stdev(scores)


