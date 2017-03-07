
from collections import defaultdict
import calendar
import datetime
import copy

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

    def main(self):
        self.extract_event_sequences()
        num_entities = len(self.entity_events.keys())
        for e,entity in enumerate(self.entity_events.keys()):
            print(entity,' - ',e,'of',num_entities)
            events = self.entity_events[entity]
            periodics = self.detect_periodicity(events)
            for periodic in periodics:
                self.save_periodicity(periodic)

    def detect_periodicity(self,events,periodics_threshold): 
        periodics = self.detect_day_periodicity(events) + self.detect_weekday_periodicity(events) + self.detect_weekday_of_month_periodicity(events)
        periodics_above_threshold = [pattern for pattern in periodics if pattern[1] > periodics_threshold]
        return self.select_periodics(sorted(periodics_above_threshold,key = lambda k : k[1],reverse=True)) if len(periodics_above_threshold) > 0 else []

    def save_periodicity(self,periodic):
        pattern = periodic[0]
        score = periodic[1]
        description = self.describe_pattern(pattern)
        for i,event in enumerate(periodic[2]):
            editions = [edition.mongo_id for edition in periodic[2]]
            event.set_periodic({'pattern':pattern,'score':score,'description':description,'editions':editions})

    def select_periodics(self,periodics):
        selection = periodics[0]
        for periodic in periodics[1:]:
            events = periodic[2]
            selection_events = [s[2] for s in selection]
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
            periodic_patterns.append([periodic_pattern,periodic_score])
        return periodic_patterns

    def detect_weekday_periodicity(self,events):
        periodic_patterns = []
        event_dates = self.return_dates(events)
        recurring_weekdays = self.detect_recurring_weekdays(event_dates)
        for weekday in recurring_weekdays: 
            candidate_events = self.return_candidate_events_weekday(events,day)
            candidate_event_dates = self.return_dates(candidate_events)
            # find yearly recurring weekday-week combis
            recurring_weeks = self.detect_recurring_weeks(candidate_event_dates)
            for week in recurring_weeks:
                candidate_events_week = self.return_candidate_events_week(events,week)
                candidate_event_dates_week = self.return_dates(candidate_events_week)
                candidate_event_year_steps = self.return_year_steps(candidate_event_dates_week)
                periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('year'),candidate_event_year_steps,[self.pattern_fields.index('week'),self.pattern_fields.index('weekday')],[week,weekday])
                periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events_week),len(events),candidate_event_year_steps)
                periodic_patterns.append([periodic_pattern,periodic_score,candidate_events])
            # find weekly recurring weekdays
            candidate_event_week_steps = self.return_week_steps(candidate_event_dates)
            periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('week'),candidate_event_month_steps,[self.pattern_fields.index('weekday')],[weekday])
            periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events),len(events),candidate_event_month_steps)
            periodic_patterns.append([periodic_pattern,periodic_score])
        return periodic_patterns

    def detect_weekday_of_month_periodicity(self,events):
        periodic_patterns = []
        event_dates = self.return_dates(events)
        recurring_weekdays = self.detect_recurring_weekdays(event_dates)
        for weekday in recurring_weekdays: 
            candidate_events = self.return_candidate_events_weekday(events,day)
            candidate_event_dates = self.return_dates(candidate_events)
            # find yearly recurring weekday-day_of_month combis
            recurring_weekdays_of_month = self.detect_recurring_weekdays_of_month(candidate_event_dates)
            for weekday_of_month in recurring_weekdays_of_month:
                candidate_events_week_of_month = self.return_candidate_events_week_of_month(candidate_events,weekday_of_month)
                candidate_event_week_of_month_dates = self.return_dates(candidate_events_week_of_month)
                recurring_months = self.detect_recurring_months(candidate_events_week_of_month)
                for month in recurring_months:
                    candidate_events_month = self.return_candidate_events_month(candidate_events_week_of_month,month)
                    candidate_event_dates_month = self.return_dates(candidate_events_month)
                    candidate_event_year_steps = self.return_year_steps(candidate_event_dates_month)
                    periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('year'),candidate_event_year_steps,[self.pattern_fields.index('month'),self.pattern_fields.index('week_of_month'),self.pattern_fields.index('weekday')],[month,weekday_of_month,weekday])
                    periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events_month),len(events),candidate_event_year_steps)
                    periodic_patterns.append([periodic_pattern,periodic_score,candidate_events_month])
                # find monthly recurring weekdays of month
                candidate_event_month_steps = self.return_month_steps(candidate_event_week_of_month_dates)
                periodic_pattern = self.return_periodic_pattern(self.pattern_fields.index('month'),candidate_event_month_steps,[self.pattern_fields.index('week_of_month'),self.pattern_fields.index('weekday')],[week_of_month,weekday])
                periodic_score = self.assess_periodicity(periodic_pattern,len(candidate_events),len(events),candidate_event_month_steps)
                periodic_patterns.append([periodic_pattern,periodic_score])
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
        step = min(constant_steps)
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
        return num_candidates / num_candidates + sum([step-standard_step for step in steps])

    def describe_pattern(self,pattern):
        # describe sequence
        time_unit_sequence = self.pattern_fields[pattern.index('e')]
        step = pattern[6]
        sequence_description = self.describe_sequence(time_unit_sequence,step)
        # describe recurring pattern
        time_units_recurring = [[i,field] for field in enumerate(pattern[:6]) if field not in ['v','e']]
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
        return sequence_description + ' ' + recurring_pattern

    def describe_recurring_weekday_week(self,weekday,week):
        'in week ' + str(week) + ' op ' + self.return_weekday_str(weekday)

    def describe_recurring_date(self,month,monthday):
        return 'op ' + str(monthday) + ' ' + self.return_month_str(month)

    def describe_recurring_month_weekday_of_month(self,month,week_of_month,weekday):
        return 'op de ' + str(week_of_month) + 'e ' + self.return_weekday_str(weekday) + ' van ' + self.return_month_str(month)

    def describe_recurring_monthday(self,monthday):
        return 'op de ' + str(monthday) + 'e'

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
        return [event.datetime for event in events]

    def return_day_sequence(self,dates):
        return [date.day for date in dates]

    def return_weekday_sequence(self,dates):
        return [date.weekday() for date in dates]

    def return_week_of_month_sequence(self,dates):
        return [self.return_week_of_month(date) for date in dates]

    def return_week_of_month(self,date):
        for i,week_of_month_days in calendar.monthcalendar(date.year,date.month):
            if date.day in week_of_month_days:
                return i+1

    def return_week_sequence(self,dates):
        return [date.isocalendar()[1] for date in dates]

    def return_month_sequence(self,dates):
        return [date.month for date in dates]

    def return_year_sequence(self,dates):
        return [date.year for date in dates]

### recurring time units

    def detect_recurring_days(self,events):
        day_sequence = self.return_day_sequence(event_dates)
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
        return [int((dates[i].year - dates[i-1].year)*12 + dates[i].isocalendar()[1] - dates[i-1].isocalendar()[1]) for i in range(1,len(dates))]
