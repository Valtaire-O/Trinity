from datetime import datetime,timedelta
from collections import Counter
import re
import dateparser


class DateOps:
    '''This class handles the use of datetime objects '''
    def __init__(self):
        self.today = datetime.today().date()

        self.convert_format = '%Y-%m-%d'
        self.time_format = '%Y-%m-%d %H:%M:%S%z'

    def convert_date(self,value):
        if value is None:
            return ''
        new_value = value
        if type(new_value) == str:

            return str(datetime.strptime(new_value, self.convert_format).date())

        if type(new_value) == list:
            new_value = new_value[0]
        date = datetime.strftime(new_value,self.convert_format)
        return date

    def convert_full_date(self, value):
        if value is None:
            return ''
        new_value = value

        return datetime.strptime(new_value, self.convert_format)

    def convert_time(self,value):
        datetime.strptime(value, self.time_format)




    def check_format(self,value):
        """make sure value matches the correct format"""
        try:
            datetime.strptime(value, self.convert_format)
        except:
            return False
        return True


    def current_date(self):
        return self.convert_date(self.today)

    def new_date(self,days):
        date = self.today + timedelta(days=days)
        return self.convert_date(date)


    def future_date(self,date):
        """Computes the distance between a provided date and the current date"""
        end_date = datetime.strptime(date, "%Y-%m-%d").date()

        return self.today <= end_date
    def date_distance(self,date):
        """Computes the distance between a provided date and the current date"""
        end_date = datetime.strptime(date, "%Y-%m-%d").date()

        distance = (abs(self.today - end_date).days)

        return distance


    def get_month(self,i,future=False):
        value = i * 31
        today =datetime.today()
        if future:
            today = today.replace(day=1) + timedelta(days=value)
        else:
            today = today.replace(day=1) -timedelta(days=value)
        return today.date()




class DateDecipher:
    '''Return datetime obj for varying formats'''

    def  __init__(self):
        self.date_ops = DateOps()
        self.months = [ "Jan","Feb","Mar","Apr","May","jun","June","Jul","July","Aug","Sept","Sep","Oct","Nov","Dec",
                        'January', 'February', 'March','April', 'May', 'August', 'September', 'October',
                        'November', 'December'
                        ]



        self.month_values, self.day_values = Counter(),Counter()

        for i in self.months:
            self.month_values[i.lower()] = 1



    def clean_text(self,value,when):

        unwanted = ['!', '@', '#', '$', '%', '&', '(', ')', '=', '~', '/', '[^\x00-\x7F]+']
        u_regex = "|".join(unwanted)


        unwanted_match = re.compile(rf'(?:{when}\s*)({u_regex})(?:\s*[0-9])', flags=re.IGNORECASE).findall(value)
        remove_list = []
        if unwanted_match:
            [remove_list.extend(i) for i in unwanted_match]

            remove_list = [i.strip() for i in remove_list if i.strip()]

            for r in remove_list:
                value = value.replace(r, '')

        return value.strip()
    def numerical_date(self, d):
        date_time_match = re.compile(r'([0-9]{4}-[0-9]{2}-[0-9]{2})', flags=re.IGNORECASE).search(d)

        if date_time_match:
            return date_time_match[0]

        """Matches numerical dates with different delimiters """
        link_match = re.compile(r'(?:/)([0-9]{1,4}\s*(\.|/|\-)\s*[0-9]{1,2}\s*(\.|/|\-)\s*[0-9]{1,4})(?:/)',
                            flags=re.IGNORECASE).search(d)
        if link_match:
            return link_match[0][1:-1]
        text_match = re.compile(r'([0-9]{1,4}\s*(\.|/|\-)\s*[0-9]{1,2}\s*(\.|/|\-)\s*[0-9]{1,4})', flags=re.IGNORECASE).search(d)
        if text_match:

            return text_match[0]

        return None
    def isolate_time(self, value, day=False):
        new_value = value

        informal_reg = '([0-9]{1,2}:[0-9]{2}\s*(am|pm))?'

        formal_reg = '((\s+[0-9]+\s*(am|pm)*\s*(-|to)\s*)?\s+[0-9]\s*(am|pm))'

        if day:
            """match day , time format"""
            cleaned_value = self.clean_text(value,day)

            possible_formats = [f"(({day}\s*{formal_reg[2:-1]})",f"({day}\s*{informal_reg[1:-2]})?"]
            for p in possible_formats:
                match = re.compile(rf'(?i){p}',
                           flags=re.IGNORECASE).search(value)
                if match:
                    return value

        informal_time = re.compile(rf'({informal_reg})',
                                   flags=re.IGNORECASE).findall(value)
        formal_time = re.compile(rf'{formal_reg}',
                                 flags=re.IGNORECASE).search(value)

        if informal_time:

            for i in informal_time:
                new_value = new_value.replace(i[0],'')

        elif formal_time:
            new_value = new_value.replace(formal_time[0].strip(), '')
        return new_value

    def alpha_date(self, value):
        """Handles dates with text months, days etc"""
        values = self.remove_char((value.lower())).split()
        n = len(values)
        #get month value from text
        month_index = [values[i] for i in range(0,n) if self.month_values[values[i].strip()]!=0]

        #Month detected in string
        if month_index:
            match_type = 'month'
            month = month_index[0]
            new_value = self.isolate_time(value)
            # use month match to find the rest of the date
            regex_one = f"({month}" + '\s+[0-9]{1,2}(?:\s*(-|to)\s*)[0-9]{1,2}(\s*,?\s*[0-9]{2,4})?)'

            regex_two = "(\s*[0-9]{1,2}(?:\s*(-|to)\s*)[0-9]{1,2}" + f"\s*,?\s*{month}" + "(\s*,?\s*[0-9]{2,4})?)"
            multi_date = re.compile(rf'(?i){regex_one}|{regex_two}',
                                    flags=re.IGNORECASE).search(new_value)

            if multi_date:
                isolated_date = multi_date[0].lower()
                vals = [' to ', '-',',',month]

                for v in vals:
                    isolated_date = isolated_date.replace(v,' ')
                isolated_arr = isolated_date.lower().split()


                if len(isolated_arr) == 2:
                    start_date = f"{month} {isolated_arr[0]}"
                    end_date = f"{month} {isolated_arr[1]}"

                    converted_start = self.date_ops.convert_date(dateparser.parse(start_date))
                    converted_end = self.date_ops.convert_date(dateparser.parse(end_date))
                    if converted_start and converted_end:
                        return {'value':converted_start, 'match_type':match_type,'og_value':[start_date,end_date]}

                elif  len(isolated_arr) == 3:
                    #len of three indicates amonth day year match
                    start_date = f"{month} {isolated_arr[0]}, {isolated_arr[2]}"
                    end_date = f"{month} {isolated_arr[1]}, {isolated_arr[2]}"

                    converted_start = self.date_ops.convert_date(dateparser.parse(start_date))
                    converted_end = self.date_ops.convert_date(dateparser.parse(end_date))

                    if converted_start and converted_end:
                        return {'value':converted_start, 'match_type':match_type,'og_value':[start_date,end_date]}


            else:

                regex_one = f"({month}" + '\s+[0-9]{1,2}(\s*,?\s*[0-9]{2,4})?)'
                regex_two = '(\s*[0-9]{1,2}' + f"\s*,?\s*{month}" + '(\s*,?\s*[0-9]{2,4})?)'

                single_date = re.compile(rf'(?i){regex_one}|{regex_two}',
                                         flags=re.IGNORECASE).search(new_value)

                if single_date:
                    isolated_date = single_date[0]
                    vals = [' to ', '-', ',']
                    for v in vals:
                        isolated_date = isolated_date.replace(v, ' ')


                    converted_date = self.date_ops.convert_date(dateparser.parse(isolated_date))
                    if converted_date:
                        return {'value':converted_date, 'match_type':match_type,'og_value':single_date[0]}


        #match text dates such as 'posted 3 days ago'
        n_time_ago = re.compile(rf'(?i)[0-9]+\s*(days|h|hr|hours?|sec|seconds?|min|m|minutes)\s*ago', flags=re.IGNORECASE).search(value)
        if n_time_ago:


            match_type = 'ago'
            converted_date = self.date_ops.convert_date(dateparser.parse(n_time_ago[0]))

            if converted_date:
                return {'value':converted_date, 'match_type':match_type, 'og_value':n_time_ago[0]}

        n_time_ago = re.compile(rf'(?i)posted\s*(today|yesterday)\s*', flags=re.IGNORECASE).search(value)
        if n_time_ago:

            match_type = 'ago'
            new_value = n_time_ago[0].split()[1]
            converted_date = DateOps().convert_date(dateparser.parse(new_value))
            if converted_date:
                return {'value': converted_date, 'match_type': match_type,'og_value':n_time_ago[0]}

        return []


    def remove_char(self,value):
        op =  ['/', '.', '-','•']
        for x in op:
            if x in value:
                value = value.replace(x, "").strip()

        return value

    def transform_date(self, value: str) -> [str]:
        """Convert the  date value to standardized format"""
        '''value = remove_tags(f'{value}')'''


        if not value:
            return {'value': '', 'match_type': '','og_value':''}
        list_value =value.split(' ')
        token_size = len([i for i in list_value if i.strip()])
        if token_size >25:

            return {'value': '', 'match_type': '','og_value':''}


        new_date = self.alpha_date(value)  # check for alpha-numerical dates
        if new_date:
            return new_date

        new_date = self.numerical_date(value)  # check for numerical dates

        if new_date:

            converted_date = self.date_ops.convert_date(dateparser.parse(new_date))
            if converted_date:
                return {'value': converted_date, 'match_type': 'numerical','og_value':new_date}



        return {'value': '', 'match_type': '','og_value':''}


