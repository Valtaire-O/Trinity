import json
import re
from typing import List,Dict
from .base_query import UrlWorks
from collections import Counter
class ConfirmTargetBlock:
    ''''Within labeled html groups, confirm that the candidate html blocks within those groups
     conform to patterns associated with each vertical'''
    def __init__(self):
        self.target_link_pattern = ''
        self.group_links = []
        self.url_works = UrlWorks()

        #define common url slugs found ammong the verticals
        self.news_slugs = ['article', 'story', 'news','post']
        self.event_slugs = ['event', 'calendar']
        self.general_neg = ['tel:', 'mailto', 'page=','email=']
        self.event_slugs_neg = ['format='] + self.general_neg
        self.news_slugs_neg = ['author',]+self.general_neg
        self.job_slugs_neg=[] + self.general_neg
        self.job_slugs = ['job', 'career', 'opening','company','opportunity','hiring']
        self.vertical_neg_map = {'news':self.news_slugs_neg, 'event':self.event_slugs_neg,
                                 'job':self.job_slugs_neg}


    def get_links(self,group,base_url,vertical):
        '''Orchestrates the pattern recognition process'''
        self.group_scores = Counter()

        targets = []
        candidates = []
        rejected_links, accepted_links,candidate_links = [], [], []
        # Focus on groups with more than 30 candidate blocks in their group as an indicator that
        # the group label is 'broad'
        if vertical =='news' and len(group) <=30:
            return group,[]
        elif vertical == 'job' and len(group) <=30:
            return group,[]
        elif vertical == 'event' and len(group) <=30:
            return group,[]

        for i in group:
            ''''assess each html block in the group'''
            links = i.find_all('a')
            block_links = []

            for l in links:
                confirm_hyper_link = self.url_works.confirm_url(l.get('href'),base_url)
                if confirm_hyper_link:
                    block_links.append(confirm_hyper_link)

            # unique list of links for each candidate block
            block_links = list(set(block_links))


            if vertical =='news':
                if len(block_links) >=4:

                    """If the len of links in block
                    are high , its likely to not be the target"""
                    continue


            block_verdict = self.analyze_links(block_links, vertical,rejected_links,accepted_links,
                                               candidate_links)
            score = block_verdict.score

            if block_verdict.reject:
                # todo: if the link analysis returns a reject, analyze other features of the block
                continue

            if block_verdict.candidate:
                candidates.append(i)

            elif block_verdict.target:

                targets.append(i)

        if not targets:
            #print(f"Identified {len(candidates)} candidates  blocks out of {len(group)} total blocks\n")
            #print(accepted_links, candidate_links,rejected_links, )
            return candidates,rejected_links
        # if the asset links come from separate domains , the source is an aggregator and there will
        # be multiple link patterns
        asset_link_domains = set([self.url_works.get_base_url(i) for i in accepted_links + candidate_links])
        if len(asset_link_domains) > 1:

            targets+=candidates

        #print(f"Identified {len(targets)} target blocks out of {len(group)} total blocks\n")
        #print(accepted_links, candidate_links,rejected_links, )
        return targets,rejected_links




    def analyze_links(self, links:List, vertical:str,rejected_links:List,accepted_links:List,candidate_links):
        '''Process list of links per candidate block,
        if at least 1 of the present links matches the pattern of its vertical return as a target or candidate '''

        for l in links:
            base_dom = self.url_works.get_base_url(l)
            sub_path = l.replace(base_dom, '').split('/')
            sub_path = [i for i in sub_path if i] # remove empty strings
            link_text = self.link_text_scanner(sub_path)
            if sub_path:
                destination_path = sub_path.pop(-1) # unique & ultimate destination of the the link
            else:
                destination_path = ''


            if self.find_neg_features(vertical=vertical,link=l,base_dom=base_dom,
                                      subpath=sub_path,destination=destination_path,link_text=link_text):
                '''if all links are neg, we end the loop with 
                the candidate & accepted list empty'''
                rejected_links.append(l)
                continue

            pos_score = self.find_pos_features(vertical=vertical,link_text=link_text,subpath=sub_path)
            if pos_score:
                accepted_links.append(l)
                return VerdictObj(target=True,score=pos_score)

            else:
                candidate_links.append(l)
        if candidate_links:
            return VerdictObj(candidate=True)
        else:
            return VerdictObj(reject=True)


    def find_pos_features(self,vertical,link_text:str,subpath:List):
        '''Look for positive features of an asset based on the verticals'''
        score = 0
        if vertical == 'news':
            vertical_subpath = self.text_patterns(self.news_slugs, ' '.join(subpath))
        elif vertical == 'event':
            vertical_subpath = self.text_patterns(self.event_slugs, ' '.join(subpath))
        else:
            vertical_subpath = self.text_patterns(self.job_slugs, ' '.join(subpath))


        if vertical_subpath:
            score+=1
        #link_identifier = self.link_text_scanner(subpath)



        link_text = link_text.split(' ')

        if len(link_text) >=3:

            score+=1
        return score

    def link_text_scanner(self,subpath:List):
        '''Get the text in a hyper link'''
        subpath.reverse()
        for s in subpath:
            match = re.compile(r'(\b-|_+\b)+', flags=re.IGNORECASE).search(s)
            if match:
                s = s.replace('-', ' ')
                s = s.replace('_', ' ')


                return s
        return ''

    def find_neg_features(self, vertical,link, subpath:List,base_dom:str,destination:str,link_text):
        '''Look for negative features of an asset based on the verticals'''
        if self.negative_match_domain(base_dom):
            return True

        subpath_text = " ".join(subpath) + destination

        if not link_text:
            '''If no title/text is found in the link check the subpath for negative features'''
            if len(subpath) == 1:
                if self.negative_match_subpath(destination):
                    return True

                unique_id = self.unique_id_pattern(destination)
                if not unique_id:
                    return True

            if len(subpath) == 2:
                if self.negative_match_subpath(subpath_text):
                    return True
        else:
            neg_subpath = self.text_patterns(self.vertical_neg_map[vertical], subpath_text)
            if neg_subpath:

                return True
        return False


    def negative_match_domain(self,domain):
        bad_domains = ['linkedin', 'instagram', 'facebook', 'twitter', 'snapchat','tiktok','google']
        check_domain = self.text_patterns(bad_domains, domain.lower())

        return check_domain

    def negative_match_subpath(self,link_text):
        '''Look for patterns of non target links'''
        bad_text = ['contact','sign','login','account','subscribe','javascript','about',
                    'privacy','agreement', 'sales','#','help','support', 'info',
                    'faqs','none','author','page','mailto','user','admin'

                    ]
        return self.text_patterns(bad_text,link_text)

    def text_patterns(self,feature_list:List,value:str):
        pattern = '|'.join([i for i in feature_list])

        match = re.compile(rf'{pattern}', flags=re.IGNORECASE).search(value)
        return match
    def unique_id_pattern(self,value):
        '''mix of letters and numbers or just numbers'''
        match = re.compile(rf'(^[a-zA-Z0-9]+(\.\w+)?$)|(^[0-9]+)|(^[a-zA-Z]+[^a-zA-Z0-9]+[0-9]+)'
        '|(^[0-9]+[^a-zA-Z0-9]+[a-zA-Z]+$)',
                           flags=re.IGNORECASE).search(value)
        return match


class ConfirmTargetBlockTest:
    ''''Within labeled html groups, confirm that the candidate html blocks within those groups
     conform to patterns associated with each vertical'''

    def __init__(self):
        self.target_link_pattern = ''
        self.group_links = []
        self.url_works = UrlWorks()

        # define common url slugs found ammong the verticals
        self.news_slugs = ['article', 'story', 'news', 'post','stories']
        self.event_slugs = ['event', 'calendar']
        self.general_neg = ['tel:', 'mailto', 'page=', 'email=']
        self.event_slugs_neg = ['format='] + self.general_neg
        self.news_slugs_neg = ['author', ] + self.general_neg
        self.job_slugs_neg = [] + self.general_neg
        self.job_slugs = ['job', 'career', 'opening', 'company', 'opportunity', 'hiring']
        self.vertical_neg_map = {'news': self.news_slugs_neg, 'event': self.event_slugs_neg,
                                 'job': self.job_slugs_neg}

    def get_links(self, group, base_url, vertical):
        '''Orchestrates the pattern recognition process'''
        self.group_scores = Counter()

        targets = []
        candidates = []
        rejected_links, accepted_links, candidate_links = [], [], []
        # Focus on groups with more than 30 candidate blocks in their group as an indicator that
        # the group label is 'broad'


        for i in group:
            ''''assess each html block in the group'''
            links = i.find_all('a')
            block_links = []

            for l in links:
                confirm_hyper_link = self.url_works.confirm_url(l.get('href'), base_url)
                if confirm_hyper_link:
                    block_links.append(confirm_hyper_link)

                #print(l.get('href'),confirm_hyper_link)


            # unique list of links for each candidate block
            block_links = list(set(block_links))


            '''if vertical == 'news':
                if len(block_links) >= 4:
                    """If the len of links in block
                    are high , its likely to not be the target"""

                    continue'''
            '''if len(block_links) >= 20:

                continue'''

            block_verdict = self.analyze_links(block_links, vertical, rejected_links, accepted_links,
                                               candidate_links)
            score = block_verdict.score

            if block_verdict.reject:
                # todo: if the link analysis returns a reject, analyze other features of the block

                continue

            if block_verdict.candidate:
                candidates.append(i)


            elif block_verdict.target:


                targets.append(i)

        if not targets:
            #print(f"Identified {len(candidates)} candidates  blocks out of {len(group)} total blocks\n")
            # print(accepted_links, candidate_links,rejected_links, )
            return candidates,rejected_links
        # if the asset links come from separate domains , the source is an aggregator and there will
        # be multiple link patterns
        asset_link_domains = set([self.url_works.get_base_url(i) for i in accepted_links + candidate_links])
        if len(asset_link_domains) > 1:
            targets += candidates

        #print(f"Identified {len(targets)} target blocks out of {len(group)} total blocks\n")
        # print(accepted_links, candidate_links,rejected_links, )
        return targets,rejected_links

    def analyze_links(self, links: List, vertical: str, rejected_links: List, accepted_links: List, candidate_links):
        '''Process list of links per candidate block,
        if at least 1 of the present links matches the pattern of its vertical return as a target or candidate '''

        for l in links:
            base_dom = self.url_works.get_base_url(l)
            sub_path = l.replace(base_dom, '').split('/')

            #sub_path = [i for i in sub_path if i]  # remove empty strings
            link_text = self.link_text_scanner(sub_path)


            if sub_path:
                destination_path = sub_path.pop(-1)  # unique & ultimate destination of the the link
            else:
                destination_path = ''

            if vertical =='news' and not link_text:
                rejected_links.append(l)

                continue

            #print(f'linkText: {link_text}')

            if self.find_neg_features(vertical=vertical, link=l, base_dom=base_dom,
                                      subpath=sub_path, destination=destination_path, link_text=link_text):
                '''if all links are neg, we end the loop with 
                the candidate & accepted list empty'''
                rejected_links.append(l)

                continue

            pos_score = self.find_pos_features(vertical=vertical, link_text=link_text, subpath=sub_path)
            if pos_score:
                accepted_links.append(l)
                return VerdictObj(target=True, score=pos_score)

            else:
                candidate_links.append(l)
        if candidate_links:
            return VerdictObj(candidate=True)
        else:
            return VerdictObj(reject=True)

    def find_pos_features(self, vertical, link_text: str, subpath: List):
        '''Look for positive features of an asset based on the verticals'''
        score = 0
        if vertical == 'news':
            vertical_subpath = self.text_patterns(self.news_slugs, ' '.join(subpath))
        elif vertical == 'event':
            vertical_subpath = self.text_patterns(self.event_slugs, ' '.join(subpath))
        else:
            vertical_subpath = self.text_patterns(self.job_slugs, ' '.join(subpath))

        if vertical_subpath:
            score += 1
        # link_identifier = self.link_text_scanner(subpath)

        link_text = link_text.split(' ')

        if len(link_text) >= 3:
            score += 1
        return score

    def link_text_scanner(self, subpath: List):
        '''Get the text in a hyper link'''
        #subpath.reverse()
        for s in subpath:
            match = re.compile(r'(\b-)+', flags=re.IGNORECASE).search(s)
            if match:
                s = s.replace('-', ' ')
                return s
        return ''

    def find_neg_features(self, vertical, link, subpath: List, base_dom: str, destination: str, link_text):
        '''Look for negative features of an asset based on the verticals'''
        if self.negative_match_domain(base_dom):
            return True

        subpath_text = " ".join(subpath) + destination

        if not link_text:
            '''If no title/text is found in the link check the subpath for negative features'''
            if len(subpath) == 1:

                if self.negative_match_subpath(destination):
                    return True

                unique_id = self.unique_id_pattern(destination)
                if not unique_id:
                    return True

            if len(subpath) == 2:
                if self.negative_match_subpath(subpath_text):
                    return True
        else:

            neg_subpath = self.text_patterns(self.vertical_neg_map[vertical], subpath_text)
            if neg_subpath:
                return True
        return False

    def negative_match_domain(self, domain):
        bad_domains = ['linkedin', 'instagram', 'facebook', 'twitter', 'snapchat', 'tiktok', 'google']
        check_domain = self.text_patterns(bad_domains, domain.lower())


        return check_domain

    def negative_match_subpath(self, link_text):
        '''Look for patterns of non target links'''
        bad_text = ['contact us', 'sign', 'login', 'account', 'subscribe', 'javascript', 'about us',
                    'privacy', 'agreement', 'sales', '#', 'help', 'support', 'info',
                    'faqs', 'none', 'author', 'page', 'mailto', 'user', 'admin'

                    ]
        text_match = self.text_patterns(bad_text, link_text)

        return text_match

    def text_patterns(self, feature_list: List, value: str):
        pattern = '|'.join([i for i in feature_list])

        match = re.compile(rf'{pattern}', flags=re.IGNORECASE).search(value)
        return match

    def unique_id_pattern(self, value):
        '''mix of letters and numbers or just numbers'''
        match = re.compile(rf'(^[a-zA-Z0-9]+(\.\w+)?$)|(^[0-9]+)|(^[a-zA-Z]+[^a-zA-Z0-9]+[0-9]+)'
                           '|(^[0-9]+[^a-zA-Z0-9]+[a-zA-Z]+$)',
                           flags=re.IGNORECASE).search(value)
        return match

class VerdictObj:
    '''Defines attributes for html blocks '''
    def __init__(self,reject=False,candidate=False,target=False, score=0):
        self.reject = reject
        self.candidate = candidate
        self.target = target
        self.score = score


