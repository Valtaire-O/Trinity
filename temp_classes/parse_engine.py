from collections import  Counter
from w3lib.html import remove_tags, replace_escape_chars
import re
#import pylev
from .date_ops import DateDecipher, DateOps
from .base_query import CloudConnect,UrlWorks
from .confirm_targets import ConfirmTargetBlock
import pytz
from pytz import timezone
import textdistance as td


class ParseEngine:
    '''Using the 'pyramid search' algorithm:
      at the top of the pyramid are the best html feature to field mappings,
      going down the pyramid, each  level contains the next best mappings.
      Once we find the initial target value , we stop the search and return
       an output for each field '''
    def __init__(self):
        self.find_date = DateDecipher()
        self.date_ops = DateOps()
        self.title_tags = ['h1' ,'h2', 'h3', 'h4', 'h5', 'h6']
        self.date_tags = ['time', 'dl', 'span','li','small'] + self.title_tags +['tr','td','div']

        self.short_desc_tags = ['p', 'span']
        self.hyper_tags = ['a']
        self.image_tags = ['img']
        self.url_works = UrlWorks()


        self.tag_map = {
            'title': self.title_tags,
            'date': self.date_tags,
            'hyper_link': self.hyper_tags,
            'image': self.image_tags,
            'short_desc': self.short_desc_tags

        }
        self.attrib_dict = {
            'a': ['href'],
            'img': ['src', 'srcset'],
            'time':['datetime'],
            'span':['date']


        }
        self.attrib_map = Counter()
        # turn dict into counter for easy subscripting
        for a in self.attrib_dict.keys():
            self.attrib_map[a] = self.attrib_dict[a]
        self.title_tag = ''
        self.time_tag = ''
        self.confirm_block = ConfirmTargetBlock()

    def get_image(self, tags, h_block, base_url):
        possible_tags = self.tag_map['image']
        likley_tags = ['src', 'srcset']
        target = []

        while likley_tags and len(target) < 1:
            l_tag = likley_tags.pop(0)

            target = [self.url_works.confirm_url(item.get(l_tag),main_url=base_url) for item in h_block.find_all('img')]
            if len(target)!=0:
                #print(target)
                if not target[0] :
                    return ''
                return list(set(target))[0]
        return ''


    def get_link(self, tags, target_block, base_url,vertical,rejects:dict):
        target_tags = [i for i in tags if i == 'a']
        a_tag_list = self.unique_attr_values(target_block.find_all('a'), 'href')
        if len(a_tag_list) ==1:
            if not rejects.get(a_tag_list[0]):
                return a_tag_list[0]


        # if only one unique a tag is present in the block, return its href value as the target
        if target_block.name == 'a' and not target_tags:
            '''The parent tag of the block is an a tag and the only one in the block'''
            target_link = self.url_works.confirm_url(target_block.get('href'), main_url=base_url)
            if target_link and not rejects.get(target_link):
                return target_link

        elif len(target_tags) ==1 :
            tag = target_block.find_all('a')[0]
            target_link = self.url_works.confirm_url(tag.get('href'), main_url=base_url)
            if target_link and not rejects.get(target_link):
                return target_link

        elif target_block.name == 'a' and  target_tags:
            '''The parent tag of the block is an a tag and there are multiple a tags within'''

            parent_link = self.url_works.confirm_url(target_block.get('href'), main_url=base_url)
            a_tag_list = parent_link+a_tag_list
        


        # in the case of multiple a tags within a block we employ heuristics to determine the target
        if self.title_tag and target_tags:
                # using the present title tag, target the  a tag of the title
                targeted_tag = f"{self.title_tag} a"

                target_link = [self.url_works.confirm_url(remove_tags(f'{item.get("href")}'), main_url=base_url) for item in
                       target_block.select(targeted_tag) if self.url_works.confirm_url(remove_tags(f'{item.get("href")}'), main_url=base_url)]

                if target_link:
                    target_link = self.unique_list_in_order(target_link)

                    if not rejects.get(target_link):
                        return target_link

        if not a_tag_list:
            return ''
        '''From many links, choose one'''
        rejected_links,accepted_links,candidate_links = [],[],[]
        self.confirm_block.analyze_links(links=a_tag_list, vertical=vertical,
                                         rejected_links=rejected_links,accepted_links=accepted_links
                                         ,candidate_links=candidate_links)
        #print(f'Link List: {a_tag_list}')
        if accepted_links:


            return accepted_links[0]

        return ''

    def unique_list_in_order(self,arr,full=False):
        '''return unique list in the original order'''
        aux = []
        for i in arr:
            if i not in aux:
                aux.append(i)
        if aux:
            if full:
                return aux

            return aux[0]
        else:
            return ''


    def unique_attr_values(self, tag_list, attr):

        unique_only = Counter()
        unique_tags = []
        for t in tag_list:
            tag_value = self.url_works.confirm_url(t.get(attr))
            if unique_only[tag_value] ==0 and tag_value:
                unique_tags.append(tag_value)
                unique_only[tag_value] = 1
            else:
                continue
        return unique_tags


    def confirm_title(self,title):
        '''Check title for dates if found, remove from title and see what's left'''
        tokenized_title = title.lower().split()


        if len(tokenized_title) <=4:
            """Flag as potential non target data,
            check for known red herrings"""
            unwanted = ['contact us','sign up', 'log in', 'register','access account','not found', 'access denied']
            for i in unwanted:


                cos_sim = td.cosine(tokenized_title, i.split())

                if cos_sim > .5:
                    return False

        if len(title) >=250:
            #exclude titles of great len
            return False
        return True

    def get_record_title(self, tags, target_block, completion=False):
        possible_tags = self.tag_map['title']
        # print(f"possible_tags for {key}: {possible_tags}")
        likley_tags = [i for i in possible_tags if i in tags]
        total_title_tags =len(likley_tags)
        if completion is True:
            '''Completion extracts from full html body behind the hyperlink of each asset'''
            for item in target_block.find_all('meta'):
                title = item.get('property')
                if title == 'og:title':
                    new_title = self.clean_text(f"{item.get('content')}")

                    return new_title
            new_title = [self.clean_text(f'{item}') for item in target_block.select('title')]
            new_title = [i  for i in new_title if i]
            if new_title:
                return new_title[0]

        if likley_tags:
            #todo Need to confirm that the correct title tag is sent in for link extraction
            #print(likley_tags)
            self.title_tag = likley_tags[0]

        if total_title_tags ==1:

            title = [self.clean_text(item.text) for item in target_block.find_all(self.title_tag) if self.clean_text(item.text)]

            for t in title:

                if self.confirm_title(t):
                    return t


        target = []
        while likley_tags and len(target) < 1:
            l_tag = likley_tags.pop(0)

            target = [self.clean_text(f'{item}') for item in target_block.find_all(l_tag)]
            target = [i for i in target if target if self.confirm_title(i)]
        if target:
            target = target[0].replace('&amp;', '&')
            #print(l_tag, target,'\n')
            return  target
        return ''

    def get_short_desc(self, tags, h_block):
        possible_tags = self.tag_map['short_desc']
        # print(f"possible_tags for {key}: {possible_tags}")
        likley_tags = [i for i in possible_tags if i in tags]
        target = []
        test = 0
        while likley_tags and len(target) < 1:
            l_tag = likley_tags.pop(0)

            target = [self.clean_text(item.text) for item in h_block.find_all(l_tag) if self.clean_text(f'{item}') != '']
        if target:
            #print(target)
            return target[0]
        return ''



    def get_date_info(self,tags, h_block, vertical, hyperlink=False):
        '''Get the date value and its origin'''
        date_value, origin, og_value = self.get_record_date(tags, h_block,vertical, hyperlink=hyperlink)
        if not date_value:
            return DateInfo()

        timezone = self.get_timezone(origin,og_value,date_value, h_block)

        hours = self.get_hours(origin, og_value, date_value, h_block)

        #Convert to UTC
        utc_date = self.convert_date(date=date_value, hours=hours,tmz=timezone)



        return DateInfo(target_date=date_value,origin=origin,tmz=timezone,hours=hours,
                        converted_date=utc_date)

    def get_record_date(self, tags, h_block, vertical, hyperlink):
            '''Starting with the most reliable origin time-datetime
                go down list of 'likely' tags  to find a date, keep track of the origin
                and return: the formatted date, its origin, and the value in its original format'''

            possible_tags = self.tag_map['date']
            likely_tags = [i for i in possible_tags if i in tags]
            if 'time' in likely_tags:
                origin = 'datetime'
                self.time_tag = likely_tags.pop(0)
                time_block = h_block.select(self.time_tag)
                if time_block:
                    time_datetime = time_block[0].get('datetime')
                    target_date = self.find_date.transform_date(time_datetime)
                    #print(target_date)
                    if target_date['value']:
                        return target_date['value'], origin, time_datetime
                    # if the datetime attr doesnt work, try again with the block text
                    target_date = self.find_date.transform_date(time_block[0].text)
                    if target_date['value']:
                        return target_date['value'], origin, time_block[0].text

            origin = 'text'
            while likely_tags:
                #Iterate throught potential target tags holding dates
                l_tag = likely_tags.pop(0)
                for item in h_block.find_all(l_tag):
                    target = self.find_date.transform_date(item.text)
                    if target['value']:
                        if vertical =='news' or vertical=='job':
                            #if the date for a news /job posting is in the future, reject it and continue the search
                            if self.date_ops.future_date(target['value']):
                                continue
                        return target['value'],origin,item.text

            # last backstop , look for a  date in the link
            if hyperlink and type(hyperlink) == str:
                target = self.find_date.transform_date(hyperlink)
                if target['value']:
                    origin = 'link'
                    # todo replace the dict tracking the regex match to logging statements
                    return target['value'], origin, hyperlink


            return None, None, None

    def get_timezone(self, origin, raw_value,date_value, h_block):
        '''Start by looking for common timezones supported by the  pyzt lib,
        if that doesnt work , look for other timezones outside of pyzt then map them back '''
        common_tmz = pytz.all_timezones
        other_tmz_map = {'PST':'America/Los_Angeles','PDT':'America/Los_Angeles'}

        other_tmz = f"({'|'.join(other_tmz_map.keys())})"
        regular_tmz = f"({'|'.join(common_tmz)})"
        '''First try to extract the timezone from the same value that the date was extracted from'''
        match = re.compile(rf'(?i){regular_tmz}', flags=re.IGNORECASE).search(raw_value)

        if match:
            return match[0]
        match = re.compile(rf'(?i){other_tmz}', flags=re.IGNORECASE).search(raw_value)
        if match:
            return other_tmz_map[match[0].upper()]

        if origin =='datetime':
            time_block = h_block.select('time')[0]
            match = re.compile(rf'(?i){regular_tmz}', flags=re.IGNORECASE).search(time_block.text)
            if match:
                return match[0]
            match = re.compile(rf'(?i){other_tmz}', flags=re.IGNORECASE).search(time_block.text)
            if match:
                return other_tmz_map[match[0]]

    def get_hours(self, origin, raw_value, date_value, h_block):
        '''first check if theres already a hours within the date value'''
        # timestamp match
        '''match = re.compile(r'((T|\s)[0-9]{2}:[0-9]{2})', flags=re.IGNORECASE).search(
            raw_value)'''
        if origin == 'datetime':
            #Todo stop repeating the time block select
            time_block = h_block.select('time')[0]
            text = time_block.text
        else:
            text = raw_value

        hours = re.compile(rf'(([0-1]?[0-9]|2[0-3]):[0-5][0-9]\s*(am|pm))', flags=re.IGNORECASE).search(text)

        if hours:
            hours = hours[0]
            # ensure non military time match never exceeds 12 hours
            if int(hours.split(':')[0]) <= 12:
                return hours
        return None
    def isolate_time_info(self, hours):
        match_meridiem =  re.compile(rf'(?i)(am|pm)', flags=re.IGNORECASE).search(hours)
        if match_meridiem:
            meridiem = match_meridiem[0]
            hours = hours.replace(meridiem,'')
            meridiem = meridiem.upper()
        else:
            meridiem = ''

        time_info = hours.split(':')
        if len(time_info) > 1:
            hour = int(time_info[0])
            minutes = int(time_info[1])

        else:
            hour = int(time_info[0])
            minutes = 00

        return  hour ,minutes, meridiem

    def to_military_time(self,hour,minute,merediem):
        '''Convert normal time to military for downstream
        requirements'''
        #Ultimate return value are hours and minutes
        if not merediem:
            return 00, minute
        # is AM and  hour is 12
        if merediem == "AM" and hour == 12:
            return 00 , minute

            # if Am and not 12,  return the time as is
        elif merediem == "AM":
            return hour, minute

            # Checking if last two elements of time
        # is PM the hour is  12, return time as is
        elif merediem == "PM" and  hour == 12:
            return hour, minute

        else:
            # add 12 to hours
            return hour +12, minute


    def clean_text(self,raw_text):
        """Take in dirty html text, output clean human readable text"""
        # replace html open close tags with extra space to avoid unwanted concatenations
        '''text = raw_text.replace('<'," <")
        text = text.replace('>', "> ")'''
        text = remove_tags(raw_text)
        text = text.encode("ascii", "ignore").decode().strip()
        text = self.replace_escape(text)
        cleaned_text = " ".join(text.split())
        bad_chars = ['(',')','|','-','@','#',':','$',',','/']

        for b in bad_chars:
            cleaned_text = cleaned_text.replace(b,' ')
        #remove empty values and spaces
        cleaned_text = ' '.join([i.strip() for i in cleaned_text.split()]).lower()
        return  cleaned_text.title()

    def replace_escape(self,t):
        """Find and replace escape characters"""
        text = t
        match = re.compile(r'\\n|\\t|\\r', flags=re.IGNORECASE).search(t)
        bad_list = ['\\n','\\t','\\r']
        if match:
            for b in bad_list:
                text = text.replace(b,' ')
        return text

    def convert_date(self, date:str, hours:str, tmz:str):
        '''Convert date from its given timezone to UTC'''
        if not tmz:
            return None
        if not hours:
            hours = '00:00'

        hour, minutes, meridiem = self.isolate_time_info(hours)
        mt_hr, mt_mn = self.to_military_time(hour, minutes, meridiem)

        current_date = self.date_ops.convert_full_date(date)
        full_date = current_date.replace(hour=mt_hr, minute=mt_mn)

        # convert to og timezone to get offset
        og_tmz = full_date.astimezone(timezone(tmz))
        convert_utc = og_tmz.astimezone(timezone('UTC'))

        # prep time for bubble , add to stage load
        '''date, time = f"{convert_utc}".split()
        time = time.split('+')[0]
        d['converted_utc_date'] = f"{date}T{time}"
        print(f"{d['converted_utc_date']} timezone: UTC\n")'''
        return convert_utc



class DateInfo:
    def __init__(self,target_date=None,converted_date=None,origin=None,tmz=None,hours=None,original_value=None):
        self.target_date =target_date
        self.converted_date = converted_date
        self.origin = origin
        self.tmz = tmz
        self.hours = hours
        self.original_value = original_value

