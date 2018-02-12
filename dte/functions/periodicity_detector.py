
from collections import defaultdict
import calendar
import datetime
import copy

from dte.classes.event import Event

class PeriodicityDetector:

    def __init__(self):
        self.events = []
        self.entity_events = defaultdict(list)
        self.pattern_fields = ['year','month','week','day','weekday','week_of_month','step'] 

    def set_events(self,events):
        self.events = events

    def add_events(self,events):
        self.events.extend(events)

    def return_events(self):
        return self.events

    def extract_event_sequences(self):
        for event in self.events:
            for entity in event.entities:
                self.entity_events[entity].append(event)

##############################
### Main functions
##############################

    def main(self,periodics_threshold):
        self.extract_event_sequences()
        num_entities = len(self.entity_events.keys())
        periodic_events = []
        for e,entity in enumerate(self.entity_events.keys()):
            events = [event for event in sorted(self.entity_events[entity],key = lambda k : k.datetime) if not event.predicted]
            periodics = self.detect_periodicity(events,periodics_threshold)
            for periodic in periodics:
                periodic_events.extend(list(set(sum([p[2] for p in periodics],[]))))
                self.save_periodicity(periodic)
                self.apply_periodicity(periodic)
        print('distinguishing aperiodic events from periodic events')
        aperiodic_events = set(self.events) - set(periodic_events)
        print('Done. Of the',len(self.events),'events,',len(periodic_events),'are periodic, and',len(aperiodic_events),'are aperiodic') 
        for aperiodic in aperiodic_events:
            aperiodic.set_cycle('aperiodic')

    def selective_periodicity(self,periodics_threshold):
        self.extract_event_sequences()
        selection = [event for event in self.events if not event.cycle]
        periodic_events = []
        for event in selection:
            for entity in event.entities:
                events = sorted(self.entity_events[entity],key = lambda k : k.datetime)
                periodics = self.detect_periodicity(events,periodics_threshold)
                for periodic in periodics:
                    periodic_events.extend(list(set(sum([p[2] for p in periodics],[]))))
                    self.save_periodicity(periodic)
                    self.apply_periodicity(periodic)
        print('distinguishing aperiodic events from periodic events')
        aperiodic_events = set(selection) - set(periodic_events)
        print('Done. Of the',len(selection),'new events,',len(periodic_events),'are periodic, and',len(aperiodic_events),'are aperiodic') 
        for aperiodic in aperiodic_events:
            aperiodic.set_cycle('aperiodic')

    def detect_periodicity(self,events,periodics_threshold): 
        periodics = self.detect_day_periodicity(events) + self.detect_weekday_periodicity(events) + self.detect_weekday_of_month_periodicity(events)
        periodics_above_threshold = [pattern for pattern in periodics if pattern[1] > periodics_threshold]
        return self.select_periodics(sorted(periodics_above_threshold,key = lambda k : k[1],reverse=True)) if len(periodics_above_threshold) > 0 else []

    def save_periodicity(self,periodic):
        pattern = periodic[0]
        score = periodic[1]
        description = self.describe_pattern(pattern)
        for i,event in enumerate(periodic[2]):
            editions = periodic[2]
            event.set_cycle('periodic')
            event.set_periodicity({'pattern':pattern,'score':score,'description':description,'editions':[{'id':edition.mongo_id,'date':str(edition.datetime.date()),'entities':', '.join(edition.entities)} for edition in editions]})

    def apply_periodicity(self,periodic): # predict future events based on periodic pattern (predict forward one edition)
        pattern = periodic[0]
        editions = periodic[2]
        last_date = max([edition.datetime for edition in editions])
        sequence_level = pattern.index('e')
        step = pattern[-1]
        if pattern[3] != 'v': # day pattern
            day = pattern[3]
            if sequence_level == 0: # yearly pattern
                year = last_date.year+step
                month = last_date.month
            elif sequence_level == 1: #monthly pattern
                month = last_date.month + step
                if month > 12:
                    year = last_date.year+1
                    month = month-12
                else:
                    year = last_date.year
            try:
                predicted_date = datetime.datetime(year,month,day)
            except:
                predicted_date = False
        else: # weekday pattern
            weekday = pattern[4]
            if pattern[2] != 'v': #week is filled
                if sequence_level == 2: # weekly pattern
                    predicted_date = last_date + datetime.timedelta(days = 7*step)
                else: # yearly pattern
                    year = last_date.year+step
                    week = pattern[2]
                    d = str(year) + '-W' + str(week) + '-' + str(weekday)
                    try:
                        predicted_date = datetime.datetime.strptime(d, '%Y-W%W-%w')
                    except:
                        predicted_date = False
            else: # weekday - week of month
                week_of_month = pattern[5]
                if sequence_level == 1: # monthly pattern
                    month = last_date.month + step
                    if month < 13:
                        year = last_date.year
                    elif month > 12 and month < 25:
                        year = last_date.year+1
                        month = month-12
                    elif month > 24 and month < 37:
                        year = last_date.year+2
                        month = month-24
                    elif month > 36:
                        year = last_date.year+3
                        month = month-36
                else: # yearly pattern
                    year = last_date.year+step
                    month = last_date.month
                subtract = 0 if (calendar.monthcalendar(year,month)[0][weekday] == 0) else 1
                try:
                    day = calendar.monthcalendar(year,month)[week_of_month-subtract][weekday]
                    predicted_date = datetime.datetime(year,month,day)
                except:
                    predicted_date = False
        if predicted_date:
            self.set_predicted_event(predicted_date,periodic)

    def set_predicted_event(self,predicted_date,periodic):
        events_date = [event for event in self.events if event.datetime.date() == predicted_date]
        events_date_entities = sum([event.entities for event in events_date],[])
        pattern = periodic[0]
        score = periodic[1]
        editions = periodic[2]
        predicted_event = Event()
        predicted_event.set_datetime(predicted_date)
        all_entities = sum([edition.entities for edition in editions],[])
        consistent_entities = [entity for entity in list(set(all_entities)) if all_entities.count(entity) == len(editions)]
        if len(consistent_entities) == 0:
            consistent_entities = [max(all_entities,key=all_entities.count)]
        if not set(consistent_entities) & set(events_date_entities):
            predicted_event.add_entities(consistent_entities)
            all_locations = [edition.location for edition in editions]
            location = max(set(all_locations), key=all_locations.count)
            if (location != False and all_locations.count(location) >= (len(editions)/2)):
                predicted_event.location = location 
            all_eventtypes = [edition.eventtype for edition in editions]
            eventtype = max(set(all_eventtypes), key=all_eventtypes.count)
            if (eventtype != False and all_eventtypes.count(eventtype) >= (len(editions)/2)):
                predicted_event.eventtype = eventtype
            predicted_event.set_cycle('periodic')
            description = self.describe_pattern(pattern)
            predicted_event.set_periodicity({'pattern':pattern,'score':score,'description':description,'editions':[{'id':edition.mongo_id,'date':str(edition.datetime.date()),'entities':', '.join(edition.entities)} for edition in editions]})
            predicted_event.set_predicted()
            predicted_event.resolve_overlap_entities()
            predicted_event.order_entities()
            self.events.append(predicted_event)

    def select_periodics(self,periodics):
        selection = [periodics[0]]
        for periodic in periodics[1:]:
            events = periodic[2]
            selection_events = sum([s[2] for s in selection],[])
            if not self.has_overlap_events(selection_events,events):
                selection.append(periodic)
        return selection

    def has_overlap_events(self,events1,events2):
        dates_event1 = self.return_dates(events1)
        dates_event2 = self.return_dates(events2)
        if len(set(dates_event1) and set(dates_event2)) > 0:
            return True
        else:
            return False

    def detect_day_periodicity(self,events):
        periodic_patterns = []
        event_dates = self.return_dates(events)
        recurring_days = self.detect_recurring_days(event_dates)
        for day in recurring_days: 
            candidate_events = self.return_candidate_events_day(events,day)
            candidate_event_dates = self.return_dates(candidate_events)
            # find yearly recurring day-month combis
            recurring_months = self.detect_recurring_months(candidate_event_dates)
            for month in recurring_months:
                candidate_events_month = self.return_candidate_events_month(candidate_events,month)
                candidate_event_dates_month = self.return_dates(candidate_events_month)
                candidate_event_year_steps = self.return_year_steps(candidate_event_dates_month)
                periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('year'),candidate_event_year_steps,[self.pattern_fields.index('month'),self.pattern_fields.index('day')],[month,day])
                periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events_month),len(events),candidate_event_year_steps)
                periodic_patterns.append([periodic_pattern,periodic_score,candidate_events_month])
            # find monthly recurring days
            candidate_event_month_steps = self.return_month_steps(candidate_event_dates)
            periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('month'),candidate_event_month_steps,[self.pattern_fields.index('day')],[day])
            periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events),len(events),candidate_event_month_steps)
            periodic_patterns.append([periodic_pattern,periodic_score,candidate_events])
        return periodic_patterns

    def detect_weekday_periodicity(self,events):
        periodic_patterns = []
        event_dates = self.return_dates(events)
        recurring_weekdays = self.detect_recurring_weekdays(event_dates)
        for weekday in recurring_weekdays: 
            candidate_events = self.return_candidate_events_weekday(events,weekday)
            candidate_event_dates = self.return_dates(candidate_events)
            # find yearly recurring weekday-week combis
            recurring_weeks = self.detect_recurring_weeks(candidate_event_dates)
            for week in recurring_weeks:
                candidate_events_week = self.return_candidate_events_week(events,week)
                candidate_event_dates_week = self.return_dates(candidate_events_week)
                candidate_event_year_steps = self.return_year_steps(candidate_event_dates_week)
                periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('year'),candidate_event_year_steps,[self.pattern_fields.index('week'),self.pattern_fields.index('weekday')],[week,weekday])
                periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events_week),len(events),candidate_event_year_steps)
                periodic_patterns.append([periodic_pattern,periodic_score,candidate_events_week])
            # find weekly recurring weekdays
            candidate_event_week_steps = self.return_week_steps(candidate_event_dates)
            periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('week'),candidate_event_week_steps,[self.pattern_fields.index('weekday')],[weekday])
            periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events),len(events),candidate_event_week_steps)
            periodic_patterns.append([periodic_pattern,periodic_score,candidate_events])
        return periodic_patterns

    def detect_weekday_of_month_periodicity(self,events):
        periodic_patterns = []
        event_dates = self.return_dates(events)
        recurring_weekdays = self.detect_recurring_weekdays(event_dates)
        for weekday in recurring_weekdays: 
            candidate_events = self.return_candidate_events_weekday(events,weekday)
            candidate_event_dates = self.return_dates(candidate_events)
            # find yearly recurring weekday-day_of_month combis
            recurring_weekdays_of_month = self.detect_recurring_weekdays_of_month(candidate_event_dates)
            for weekday_of_month in recurring_weekdays_of_month:
                candidate_events_week_of_month = self.return_candidate_events_week_of_month(candidate_events,weekday_of_month)
                candidate_event_week_of_month_dates = self.return_dates(candidate_events_week_of_month)
                recurring_months = self.detect_recurring_months(candidate_event_week_of_month_dates)
                for month in recurring_months:
                    candidate_events_month = self.return_candidate_events_month(candidate_events_week_of_month,month)
                    candidate_event_dates_month = self.return_dates(candidate_events_month)
                    candidate_event_year_steps = self.return_year_steps(candidate_event_dates_month)
                    periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('year'),candidate_event_year_steps,[self.pattern_fields.index('month'),self.pattern_fields.index('week_of_month'),self.pattern_fields.index('weekday')],[month,weekday_of_month,weekday])
                    periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events_month),len(events),candidate_event_year_steps)
                    periodic_patterns.append([periodic_pattern,periodic_score,candidate_events_month])
                # find monthly recurring weekdays of month
                candidate_event_month_steps = self.return_month_steps(candidate_event_week_of_month_dates)
                periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('month'),candidate_event_month_steps,[self.pattern_fields.index('week_of_month'),self.pattern_fields.index('weekday')],[weekday_of_month,weekday])
                periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events),len(events),candidate_event_month_steps)
                periodic_patterns.append([periodic_pattern,periodic_score,candidate_events_week_of_month])
        return periodic_patterns

##############################
### Helper functions
##############################

### periodic pattern functions

    def return_periodic_pattern(self,sequence_level,sequence_steps,periodic_levels,periodic_values):
        # initialize pattern
        pattern = ['v','v','v','v','v','v','v']
        # set sequence level
        pattern[sequence_level] = 'e'
        # set periodic levels
        for i,periodic_level in enumerate(periodic_levels):
            pattern[periodic_level] = periodic_values[i]
        # decide step size
        step = min(sequence_steps)
        if step == 0:
            step = 1
        pattern[6] = step
        return pattern

    def assess_periodicity(self,pattern,num_candidates,num_events,steps):
        # score coverage
        support = self.score_support(num_candidates,num_events)
        # score confidence
        confidence = self.score_confidence(num_candidates,steps,pattern[self.pattern_fields.index('step')])
        # calculate periodicty score
        score = (support+confidence) / 2
        return score

    def score_support(self,num_candidates,num_events):
        return num_candidates / num_events

    def score_confidence(self,num_candidates,steps,standard_step):
        return num_candidates / (num_candidates + sum([step-standard_step for step in steps]))

    def describe_pattern(self,pattern):
        # describe sequence
        time_unit_sequence = self.pattern_fields[pattern.index('e')]
        step = pattern[6]
        sequence_description = self.describe_sequence(time_unit_sequence,step)
        # describe recurring pattern
        time_units_recurring = [[i,field] for i,field in enumerate(pattern[:6]) if field not in ['v','e']]
        fields_recurring_timeunits = [x[0] for x in time_units_recurring]
        if 3 in fields_recurring_timeunits: # pattern with date
            day = time_units_recurring[-1][1]
            if 1 in fields_recurring_timeunits: # month is also present
                month = time_units_recurring[0][1]
                recurring_pattern = self.describe_recurring_date(month,day)
            else: # monthly pattern
                recurring_pattern = self.describe_recurring_monthday(day) 
        elif 4 in fields_recurring_timeunits: # pattern with weekday            
            if 5 in fields_recurring_timeunits: # week of monthday
                week_of_month = time_units_recurring[-1][1]
                weekday = time_units_recurring[-2][1]
                if 1 in fields_recurring_timeunits: # month is also present
                    month = time_units_recurring[0][1]
                    recurring_pattern = self.describe_recurring_month_weekday_of_month(month,week_of_month,weekday)
                else: # monthly pattern
                    recurring_pattern = self.describe_recurring_weekday_of_month(week_of_month,weekday)
            else: # weekpattern
                weekday = time_units_recurring[-1][1]
                if 2 in fields_recurring_timeunits: # week is also present
                    week = time_units_recurring[0][1]
                    recurring_pattern = self.describe_recurring_weekday_week(weekday,week)
                else: # weekly pattern
                    recurring_pattern = self.describe_recurring_weekday(weekday)
        # print('time units recurring:',time_units_recurring,'fields recurring timeunits',fields_recurring_timeunits)
        # print('pattern-',pattern,'sequence_description-',sequence_description,'recurring_pattern-',recurring_pattern)
        return sequence_description + ' ' + recurring_pattern

    def describe_recurring_weekday_week(self,weekday,week):
        return 'in week ' + str(week) + ' op ' + self.return_weekday_str(weekday)

    def describe_recurring_date(self,month,monthday):
        return 'op ' + str(monthday) + ' ' + self.return_month_str(month)

    def describe_recurring_month_weekday_of_month(self,month,week_of_month,weekday):
        return 'op de ' + str(week_of_month) + 'e ' + self.return_weekday_str(weekday) + ' van ' + self.return_month_str(month)

    def describe_recurring_monthday(self,monthday):
        return 'op de ' + str(monthday) + 'e dag'

    def describe_recurring_weekday(self,weekday):
        return 'op ' + self.return_weekday_str(weekday)

    def describe_recurring_weekday_of_month(self,week_of_month,weekday):
        return 'op de ' + str(week_of_month) + 'e ' + self.return_weekday_str(weekday)

    def describe_sequence(self,timeunit,step):
        timeunit_dutch = {'year':'jaar','month':'maand','week':'week'}
        timeunit_plural = {'jaar':'jaren','week':'weken','maand':'maanden'}
        timeunit = timeunit_dutch[timeunit]
        repeat = 'iedere' if (timeunit == 'week' or timeunit == 'maand') else 'ieder'
        value = timeunit if step == 1 else str(step) + ' ' + timeunit_plural[timeunit]
        return repeat + ' ' + value

    def return_weekday_str(self,weekday):
        weekday_str = {0:'maandag',1:'dinsdag',2:'woensdag',3:'donderdag',4:'vrijdag',5:'zaterdag',6:'zondag'}
        return weekday_str[weekday]

    def return_month_str(self,month):
        month_str = {1:'januari',2:'februari',3:'maart',4:'april',5:'mei',6:'juni',7:'juli',8:'augustus',9:'september',10:'oktober',11:'november',12:'december'}
        return month_str[month]

### get information units from events or dates

    def return_dates(self,events):
        return sorted(list(set([event.datetime.date() for event in events])))

    def return_day_sequence(self,dates):
        return [date.day for date in list(set(dates))]

    def return_weekday_sequence(self,dates):
        return [date.weekday() for date in list(set(dates))]

    def return_week_of_month_sequence(self,dates):
        return [self.return_week_of_month(date) for date in list(set(dates))]

    def return_week_of_month(self,date):
        add = 0 if (calendar.monthcalendar(date.year,date.month)[0][date.weekday()]) == 0 else 1
        for i,week_of_month_days in enumerate(calendar.monthcalendar(date.year,date.month)):
            if date.day in week_of_month_days:
                week = i 
                return i+add

    def return_week_sequence(self,dates):
        return [date.isocalendar()[1] for date in list(set(dates))]

    def return_month_sequence(self,dates):
        return [date.month for date in list(set(dates))]

    def return_year_sequence(self,dates):
        return [date.year for date in list(set(dates))]

### recurring time units

    def detect_recurring_days(self,dates):
        day_sequence = self.return_day_sequence(dates)
        candidates = [day for day in list(set(day_sequence)) if day_sequence.count(day) > 2]
        return candidates

    def detect_recurring_weekdays(self,dates):
        weekday_sequence = self.return_weekday_sequence(dates)
        candidates = [weekday for weekday in list(set(weekday_sequence)) if weekday_sequence.count(weekday) > 2]
        return candidates

    def detect_recurring_weekdays_of_month(self,dates):
        week_of_month_sequence = self.return_week_of_month_sequence(dates)
        candidates = [week_of_month for week_of_month in list(set(week_of_month_sequence)) if week_of_month_sequence.count(week_of_month) > 2]
        return candidates

    def detect_recurring_months(self,dates):
        month_sequence = self.return_month_sequence(dates)
        candidates = [month for month in list(set(month_sequence)) if month_sequence.count(month) > 2]
        return candidates

    def detect_recurring_weeks(self,dates):
        week_sequence = self.return_week_sequence(dates)
        candidates = [week for week in list(set(week_sequence)) if week_sequence.count(week) > 2]
        return candidates

### events by time-unit

    def return_candidate_events_day(self,events,day):
        return [event for event in events if event.datetime.day == day]
        
    def return_candidate_events_weekday(self,events,weekday):
        return [event for event in events if event.datetime.weekday() == weekday]

    def return_candidate_events_week_of_month(self,events,week_of_month):
        return [event for event in events if self.return_week_of_month(event.datetime) == week_of_month]

    def return_candidate_events_month(self,events,month):
        return [event for event in events if event.datetime.month == month]

    def return_candidate_events_week(self,events,week):
        return [event for event in events if event.datetime.isocalendar()[1] == week]

### intervals between events given a time unit

    def return_year_steps(self,dates):
        return [int((dates[i]-dates[i-1]).days / 365) for i in range(1,len(dates))]

    def return_month_steps(self,dates):
        return [int((dates[i].year - dates[i-1].year)*12 + dates[i].month - dates[i-1].month) for i in range(1,len(dates))]

    def return_week_steps(self,dates):
        return [int((dates[i].year - dates[i-1].year)*52 + dates[i].isocalendar()[1] - dates[i-1].isocalendar()[1]) for i in range(1,len(dates))]
