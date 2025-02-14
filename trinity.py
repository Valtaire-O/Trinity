import json
from temp_classes.parse_engine import ParseEngine
from bs4 import BeautifulSoup

from more_itertools import sliced
from temp_classes.base_query import UrlWorks
from html_classifiers import NaiveBayes, LogReg
from temp_classes.confirm_targets import ConfirmTargetBlockTest, ConfirmTargetBlock
from db_quieries import StoragePipeline
from typing import Dict, List
from temp_classes.async_api import Asnyc_API
import textdistance as td
from cleaners import TextWorks

from time import  perf_counter
class Trinity:
    def __init__(self):
        """Define class instances for tag mapping, and candidate lists"""
        self.candidates, self.rejects = [], []
        self.confirm_block = ConfirmTargetBlockTest()
        self.parse_engine = ParseEngine()
        #used during testing
        self.pos_samples, self.neg_samples = {}, {}
        self.bayesian_model = NaiveBayes()
        self.logistic_model = LogReg()
        self.url_works = UrlWorks()
        self.async_api = Asnyc_API()
        self.dataset_dir = 'datasets/'
        self.text_works = TextWorks()
        self.storage = StoragePipeline()

    def get_source_html(self):
        '''with open(f'{self.dataset_dir}Unlabeled_data.json') as f:
            data = json.load(f)["Row"][:200]'''
        data = self.storage.get_sources()

        for row in data:
            url = row['url']
            row['url'] = self.async_api.api_url() + url + self.async_api.api_params() + \
                         self.async_api.api_key()

            # scrape sources and return html
        ready_data = Asnyc_API().make_calls(data, headers={}, scrape_state=True, base_limit=48)

        '''js = open(f'test_set.json', "w")
        js.write(json.dumps({"Row": ready_data}, indent=2, sort_keys=True))
        js.close()'''
        return ready_data

    def find_target_region(self):
        '''grab sources in need of processing'''

        '''make async requests'''
        scraped_data  = self.get_source_html()
        '''with open(f'test_set.json') as f:
            scraped_data = json.load(f)["Row"]'''

        for t in scraped_data:
            source_id = t['source_id']
            base_url = t['base_url']
            vertical = t['asset']
            html = t['response']['html']
            status =t['response']['status']
            self.storage.add_batch(source_id, status)
            if status!=200 or not html:
                # update in db
                self.storage.update_label(source_id,'')
                continue

            candidate_blocks, rejects = self.segmentation_layer(html, base_url=base_url, vertical=vertical,
                                                                source_id=source_id)
            predicted_regions = self.classification_layer(candidate_blocks)
            label = self.output_layer(predicted_regions,candidate_blocks)
            self.storage.add_batch(source_id, status)
            self.storage.update_label( source_id,label)
            print(source_id)
            #print(candidate_blocks)

            '''for i in output[key]:
                i['batch_id'] = value
                rec_count += 1
                if rec_count >= 6:
                    break
                try:
                    storage.persist_items(i)
                except:'''


    def segmentation_layer(self, html, base_url, source_id, vertical):
        """Separate common Background/noisy regions from potential
        target rich regions in the html dom"""
        soup = BeautifulSoup(f"{html}", 'html.parser')
        leaf_nodes = soup.find_all('a')
        # get rid of dupes whilst maintaining order in the list
        leaf_nodes = self.parse_engine.unique_list_in_order(leaf_nodes, full=True)
        parent_map, already_seen = {}, {}
        rejects, block_count = 0, 0

        for lf in leaf_nodes:
            link = lf.get('href')
            # skip duplicate links
            if not already_seen.get(link):
                already_seen[link] = 1
            else:
                continue
            if not link or link == '/':
                continue

            parent_block = lf.parent
            parent_block = self.analyze_block(parent_block, vertical, source_id)
            block_identifier = parent_block.name

            if parent_block.passed == False:
                block_count += 1
                rejects += 1
                continue

            if not parent_map.get(block_identifier):
                block_count += 1
                parent_map[block_identifier] = parent_block
                parent_map[block_identifier].children = []

            child_block = self.analyze_block(lf, vertical, source_id)
            if child_block.passed == False:
                continue

            link_results = self.analyze_link_text(link, base_url, vertical)
            if not link_results:
                continue

            link_text, link_tokens,sub_path = link_results
            # Add the child block to the parent group
            parent_map[block_identifier].children.append(child_block.full_block.parent)
            parent_map[block_identifier].repetitions += 1
            parent_map[block_identifier].block_num = block_count
            parent_map[block_identifier].avg_link_tokens.append(link_tokens)
            parent_map[block_identifier].subpaths.append(sub_path)

        # blocks who've had all their children rejected are rejected aswell
        no_children = [1 for i in parent_map.keys() if not parent_map[i].children]
        rejects += len(no_children)

        return self.feature_extraction_layer(parent_map, rejects, base_url, vertical, source_id, html)

    def feature_extraction_layer(self, parent_map, rejects, base_url, vertical, source_id, html):
        """Take in candidate blocks, parse asset fields to create
        the output of the block, and extract relevant features"""
        candidate_blocks = {}
        for i in parent_map.keys():
            if parent_map[i].repetitions <= 100 and parent_map[i].children:
                output = self.extract_from_region(parent_map[i].children, base_url, vertical)

                if not output:
                    rejects += 1
                    continue

                pos_feature, output_count, completeness, output_links = self.find_completeness(output, parent_map[i])
                full_block = self.find_relevant_ancestors(parent_map[i].children, vertical, source_id, html, output)
                candidate_blocks[i] = {'output': output}

                # pre-process block text features
                avg_link_text = int(self.get_percentage(sum(parent_map[i].avg_link_tokens),
                                                        len(parent_map[i].avg_link_tokens)))
                attrs = [' '.join(full_block.attrs)]
                region_text = [' '.join(full_block.region_text)]

                #pre-process output link features
                resource_paths = []
                [resource_paths.extend(i[:-1]) for i in parent_map[i].subpaths]
                resource_paths = self.text_works.clean_text(' '.join(resource_paths),link=True)

                destination_slugs = self.text_works.clean_text(' '.join([i[-1] for i in parent_map[i].subpaths],),link=True)

                # classify text & attr values  with  bayes classifier
                classified_text = self.bayesian_model.classify_text_tokens(region_text)
                classified_attrs = self.bayesian_model.classify_attr_tokens(attrs)
                #classify link text
                classified_subpaths =  self.bayesian_model.classify_subpaths([resource_paths])
                classified_destination_slugs = self.bayesian_model.classify_destination_slug([resource_paths])



                candidate_blocks[i]["features"] = {"attrs": attrs, "text_lines": full_block.total_lines,
                                                   "avg_tokens": full_block.avg_tokens,
                                                   "html_feature": pos_feature, "completeness": completeness,
                                                   "output_count": output_count, "relative_position": "",
                                                   "ancestors": len(full_block.ancestors),
                                                   'avg_link_text': avg_link_text,"subpaths":parent_map[i].subpaths,
                                                   "region_text": region_text,
                                                   "resource_paths":resource_paths, "destination_slugs":destination_slugs,
                                                   "classified_subpaths": classified_subpaths,
                                                   "classified_destinations": classified_destination_slugs,

                                                   "classified_attrs": classified_attrs,
                                                   "classified_text": classified_text,}

        return candidate_blocks, rejects
    def classification_layer(self,candidate_blocks:Dict):
        classified_candidates = []

        for i in candidate_blocks.keys():

            '''access extracted features to send to model for predictions'''
            extracted_features: Dict = candidate_blocks[i]["features"]
            attrs = extracted_features["classified_attrs"]
            text = extracted_features["classified_text"]
            ancestors = extracted_features['ancestors']
            output_count = extracted_features["output_count"]
            avg_tokens = extracted_features["avg_tokens"]
            completeness = extracted_features["completeness"]
            # html_feature = extracted_features["html_feature"]
            subpaths = extracted_features["classified_subpaths"]
            destinations = extracted_features["classified_destinations"]
            avg_link_text = extracted_features["avg_link_text"]
            names = ["attrs", "text", "ancestors",
                     "output_count", "avg_tokens",
                     "completeness", "avg_link_text", "link_subpaths", "link_destinations"]

            features = [attrs, text, ancestors, output_count, avg_tokens, completeness,
                        avg_link_text, subpaths, destinations]

            # based on the input features, predict the probability of
            # each regions chances of containing
            # the target data
            prediction = self.logistic_model.make_prediction(names, features)
            if prediction:
                # self.output_layer(feature_set, output, vertical, source_id)
                classified_candidates.append(i)

        return classified_candidates

    def find_relevant_ancestors(self, block, vertical, source_id, html, output):
        region_text, region_sentences, region_attrs, ancestors = [], [], [], []
        negative_features = []
        parent = block[0].parent

        # Keep going up the tree to get all ancestors of the candidate blocks
        while parent:
            # Collect block level features on each ancestor
            block_level_features = self.analyze_block(parent, vertical, source_id)
            attrs = block_level_features.attrs

            ancestors.append(block_level_features.name)
            if not block_level_features.passed:
                negative_features.append(1)
                #print(f'neg was found at this level of parentage: {len(ancestors)}  total neg: {negative_features}')
            if attrs:
                cleaned_attrs = self.text_works.clean_text(' '.join(attrs))
                region_attrs.extend(cleaned_attrs)
            text = block_level_features.text

            if text:
                cleaned_text = self.text_works.clean_text(text)
                region_text.append(' '.join(cleaned_text))
                # newlines indicate end of sentence/phrase
                if '\n' in text:
                    text_lines = []
                    out = text.split('\n')
                    # ensure none of the newlink text chunks exceed 80 chars
                    [text_lines.extend(list(sliced(i, 80))) for i in out]
                else:
                    text_lines = list(sliced(text, 80))

                text_lines = [i.strip() for i in text_lines if i.strip()]
                region_sentences.extend(text_lines)

            parent = parent.parent
            if parent:
                if parent.name == 'body':
                    break

        # self.find_limit(html,ancestors,output)
        region_sentences = list(set(region_sentences))
        total_sentences = len(region_sentences)
        sentence_len = [len(i.split()) for i in region_sentences]
        avg_sentence_len = int(self.get_percentage(sum(sentence_len), total_sentences))

        return RegionObj(ancestors=ancestors, total_lines=total_sentences, avg_tokens=avg_sentence_len
                         , attrs=region_attrs, region_text=list(set(region_text)),negative_features=negative_features)

    def find_completeness(self, output, parent):
        """give each field a weight to represent its prevalence of occurring on the block """
        weighted_fields = {"title": .50,
                           "date": .30,
                           "image": .10,
                           "description": .10}

        output_links = list(set([i['hyper_link'] for i in output]))
        output_count = len(output_links)

        output_len = len(output)
        # get the avg amount of times a feild shows up in the output
        dates = self.get_percentage(len([i for i in output if i['date']]), output_len)
        titles = self.get_percentage(len([i for i in output if i['title']]), output_len)
        descriptions = self.get_percentage(len([i for i in output if i['short_desc']]), output_len)
        images = self.get_percentage(len([i for i in output if i['image']]), output_len)
        fields = {"title": titles,
                  "date": dates,
                  "image": images,
                  "description": descriptions}
        # in order for each feild tocount, it must  show up in 50% or more of the output
        completeness = sum([weighted_fields.get(i) for i in fields.keys() if fields[i] >= .50])

        if parent.positive_feature:
            pos_feature = 1
        else:
            pos_feature = 0
        return pos_feature, output_count, completeness, output_links

    def analyze_block(self, block, vertical, source_id ):
        '''Top level feature tags'''
        block_text = block.text.replace('\\n', '').strip()
        present_tags = [tag.name for tag in block.find_all()]
        negative = ['nav', 'header', 'footer', 'dropdown', 'breadcrumb', 'navigation', 'sidebar', 'advertisement',
                    'widget', 'cat', 'menu','module']
        positive = ['media', 'view', 'row', 'col', 'li', 'ul',
                    'article', 'main', 'tr', 'tbody','content']
        positive_feature = None

        class_values = block.get('class')
        tag_id = block.get('id')
        if class_values:
            tag_class = class_values[0].lower()
            block_identifier = '.'.join([block.name, tag_class])
            pos = self.text_works.text_patterns(positive, tag_class)
            if pos:
                positive_feature = pos[0]
        else:

            block_identifier = block.name
            class_values = []

        if tag_id:
            pos = self.text_works.text_patterns(positive, tag_id)
            if pos:
                positive_feature = pos[0]
            tag_id = [tag_id]
        else:
            tag_id = []

        attrs = [i for i in class_values + tag_id if i]
        if [i for i in present_tags if i in negative]:
            return CandidateBlock(source_id=source_id, full_block=block, name=block_identifier,attrs=attrs)

        if self.text_works.text_patterns(negative, ' '.join(attrs)):
            return CandidateBlock(source_id=source_id, full_block=block, name=block_identifier,attrs=attrs)
        if self.text_works.text_patterns(positive,' '.join(attrs)):
            positive_feature = True
        return CandidateBlock(passed=True, name=block_identifier, present_tags=present_tags,
                              full_block=block, source_id=source_id, text=block_text,
                              positive_feature=positive_feature, attrs=attrs)

    def analyze_link_text(self, link, base_url, vertical):
        verify_link = self.url_works.confirm_url(link, base_url)
        if not verify_link:
            return None

        domain = self.url_works.get_base_url(link)
        if domain:
            """filter domains on negative features"""
            weak_negatives = self.text_works.negative_match_domain(domain, base_url)
            if weak_negatives:
                return None
            sub_path = link.replace(domain, '')
        else:
            sub_path = link

        sub_path = sub_path.split('/')
        sub_path = [i for i in sub_path  if i]

        # extract link text from hyper link
        link_text = self.confirm_block.link_text_scanner(sub_path)
        link_text = link_text.split()

        if len(link_text) <= 3:
            med_negatives = self.text_works.negative_match(' '.join(link_text))
            if med_negatives:
                return None

        if not link_text:
            med_negatives = self.text_works.negative_match(' '.join(sub_path))
            if med_negatives:
                return None

        if not sub_path:
            #print(sub_path, link)
            return None
        word_count = len(link_text)
        return link_text, word_count, sub_path

    def extract_from_region(self, proposal, base_url, asset, labeled=False):
        '''Parse out relevant fields from candidate html regions'''

        target_blocks, rejected_links = self.confirm_block.get_links(proposal, base_url, asset)
        # print(target_blocks)
        rejected_links = list(set(rejected_links))
        rejects = {}
        unique_link = {}
        for r in rejected_links:
            rejects[r] = 1

        output = []
        for block in target_blocks:
            '''Using the present tags & attributes in the target blocks, dynamically extract
                            present fields'''

            all_tags = [tag.name for tag in block.find_all()]
            tags = list(set(all_tags))

            short_desc = self.parse_engine.get_short_desc(tags, block)
            if len(short_desc.split()) < 3:
                short_desc = ''

            image = self.parse_engine.get_image(tags, block, base_url)
            title = self.parse_engine.get_record_title(tags, block)
            hyper_link = self.parse_engine.get_link(all_tags, block, base_url, asset, rejects)
            date = self.parse_engine.get_date_info(tags, block, asset, hyperlink=hyper_link)

            record = {"date": date.target_date,
                      "title": title, "image": image,
                      'hyper_link': hyper_link,
                      "short_desc": short_desc
                      }
            # Only accept assets with hyperlinks as the min first pass requirement
            if hyper_link and not unique_link.get(hyper_link):
                output.append(record)

        return output

    def default_value_arr(self):
        return []

    def get_percentage(self, m: int, n: int):
        # calculate percentages safe from division by zero error
        if m == 0 or n == 0:
            return 0
        return (m / n)

    def output_layer(self,labels,candidates):
        '''Do last due diligence check for
         negative features  on block & its output
        '''

        bad_destinations = ['careers','event','job','team','faqs','contact','about us','news','index'
                           'members','terms','resources']
        bad_destinations.sort()
        choice = []
        for l in labels:
            region_data = candidates[l]

            output = region_data['output']
            feature_set = region_data['features']

            block_subpaths = feature_set['subpaths']
            destination_slugs = [i[-1] for i in block_subpaths]
            resource_paths = [i[:-1] for i in block_subpaths]
            if not destination_slugs:
                '''Give a zero score'''
                continue
            destination_slugs.sort()
            cosine = td.cosine(destination_slugs, bad_destinations)
            if cosine > 0.10:
                continue
            avg_tokens = feature_set['avg_tokens']
            positive_feature = feature_set['html_feature']

            completeness = feature_set['completeness']

            '''if  positive_feature:
                print(output)'''

            """

            #print(resource_paths)


            avg_tokens = feature_set['avg_tokens']
            positive_feature = feature_set['html_feature']
            ancestors = feature_set['ancestors']
            completeness= feature_set['completeness']
            '''print(avg_tokens,positive_feature,completeness)
            print(json.dumps(output),'/n')'''




            '''Score up positive features'''


            '''add to PQ'''
            return 1"""
            return l
        return ''

class CandidateBlock:
    def __init__(self, source_id, name=None, present_tags=None, full_block=None, attrs=None, text=None, passed=False,
                 positive_feature=None):
        self.name = name
        self.present_tags = present_tags
        self.full_block = full_block
        self.attrs = attrs
        self.text = text
        self.children = []
        self.repetitions = 1
        self.block_num = 0
        self.avg_link_tokens = []
        self.subpaths = []
        self.passed = passed
        self.source_id = source_id
        self.positive_feature = positive_feature

class RegionObj:
    def __init__(self, total_lines=0, ancestors=None, negative_features=False,
                 attrs=None, avg_tokens=0, region_text=None):
        self.total_lines = total_lines
        self.avg_tokens = avg_tokens
        self.ancestors = ancestors
        self.negative_features = negative_features
        self.attrs = attrs
        self.region_text = region_text

# run once to save the html from the  sources
# Trinity().get_source_html()

'''start = perf_counter()
Trinity().find_target_region()
stop = perf_counter()
print(f'finished in {stop-start} seconds')'''