from datetime import datetime,timedelta
from temp_classes.base_query import BaseQuery, UrlWorks
from temp_classes.date_ops import DateOps, DateDecipher
from collections import Counter
import random

class StoragePipeline(BaseQuery):
    '''Called on every yield of the spider, adds records to db'''

    def __init__(self):

        super().__init__()
        self.url_works = UrlWorks()
        self.dupes = {}
        self.today = datetime.today().date()
        links = self.curr.execute("""select hyper_link from distribution""").fetchall()

        for l in links:
            self.dupes[l[0]] = 1



    def persist_items(self, item):
        '''Insert extracted, Transformed, & Validated data into the DB'''

        if self.dupes.get(item["hyper_link"]) == 1:

            print(f"Duplicate hyperlink : {item['hyper_link']}")
            return 0

        if not item['date']:
            item['start_date'] = ''
        else:
            item['start_date'] = item['date']

        print(f'Storing: {item}')
        insert = self.curr.execute(
            """INSERT Into distribution (title,short_desc,start_date,end_date,image,hyper_link,location,batch_id,all_text,long_desc,relevant,ready,status,
            hashed_keys,raw_keys,location_check,content_check) 
            Values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
            (
                item["title"],
                item["short_desc"],
                item["start_date"],
                '',  # end dates
                item["image"],
                item["hyper_link"],
                '',
                item["batch_id"],
                '',  # all text
                '',  # long_desc
                1,#default relevant
                0,
                0,
                '[]',
                '[]',
                1,
                1
            ))

        self.dupes[item["hyper_link"]] = 1
        date_info = item['date_info']
        dist_id = insert.lastrowid
        self.curr.execute(
            """INSERT Into date_aux (original_value, origin, timezone, converted_date, hours, dist_id) 
            Values( %s,%s,%s, %s,%s,%s) """,
            (
                date_info.target_date,
                date_info.origin,
                date_info.tmz,
                date_info.converted_date,
                date_info.hours,
                dist_id
            ))
        self.config.commit()


    def pull_from_staging(self):

       source_data =  self.supabase.table('sources').select('*').eq("run",True).execute()

       ecosystems = self.supabase.table('prod_data').select('bubble_id',
                     'ecosystems').execute()
       parent_map = {}
       #map the parent id to the  ecosystems it serves

       #todo: confirm every ecosystem present in staging db is also present in trinity's

       unique_eco = []
       eco_map = self.get_eco_ids()
       for e in ecosystems.data:
           eco_names = e['ecosystems']
           if not eco_names:
               continue
           parent_map[e['bubble_id']] = [eco_map.get(i) for i in eco_names if eco_map.get(i)]
           if not parent_map[e['bubble_id']]:
               print(eco_names)
           unique_eco.extend(ecosystems)

       #confirm all ecosystems in staging db are also in trinity's db

       dupe_source = {}
       sources = []
       for d in source_data.data:
           source_url = d['source_url']
           if len(source_url) >299:
               continue

           parent_asset = d['parent_asset']
           asset = d['subtype']
           prod_id = d['id']
           origin = self.url_works.get_base_url(d['origin'])
           if not dupe_source.get(source_url):
               record = {"source_url":source_url,"parent_asset":parent_asset,
                               "asset":asset,"prod_id":prod_id,"origin":origin}

               record['eco_ids'] = parent_map.get(parent_asset)
               if not record['eco_ids']:
                   continue




               self.insert_supplier(record)
               self.supabase.table('sources').update({"run": False}).eq("id", prod_id).execute()
               dupe_source[source_url] = 1


       #return sources
    def insert_supplier(self, data):
        base_url = data['origin']
        production_id = data['parent_asset']

        if not data['origin']:
            return 0
        supplier_name = data['origin'].replace('https://', '') +'_asset'

        already_exists = self.curr.execute(f"""Select  supplier_id from  supplier 
        where base_url= %s""",(base_url)).fetchall()
        if already_exists:
            supplier_id = already_exists[0][0]
            data['supplier_id'] = supplier_id
        else:
            insert = self.curr.execute(f""" insert into supplier 
                    (supplier_name, base_url, production_id, start_page, default_logo, 
                    extraction_type, source_type, news_paginator, 
                    job_paginator, event_paginator) value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
                                                    (
                                                        supplier_name,
                                                        base_url,
                                                        production_id,
                                                        2,
                                                        '',
                                                        'general',
                                                        'local',
                                                        '',
                                                        '',
                                                        ''
                                                    ))
            supplier_id = insert.lastrowid
            data['supplier_id'] = supplier_id
            self.config.commit()


        self.insert_sources(data)
        self.config.commit()
        return 0

    def insert_sources(self,data):

        source_url = data['source_url']
        asset = data['asset']
        target_block = ''
        production_id = data['prod_id']

        supplier_id = data['supplier_id']
        already_exists = self.curr.execute(f"""Select source_id from  supply
                where url= %s""", (source_url)).fetchall()
        already_exists = [i[0] for i in already_exists]

        if  already_exists:
            data['source_id'] = already_exists[0]
        else:

            insert = self.curr.execute(f""" INSERT INTO supply
                                                (asset, url, supplier_id, checkup,target_block,production_id,live,
                                                needs_processing,done)
                                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);
    
                                                         """,
                                           (
                                               asset,
                                               source_url,
                                               supplier_id,
                                               f'{self.today}',
                                               target_block,
                                               production_id,
                                               0,
                                               1,
                                               0

                                           ))
            data['source_id'] = insert.lastrowid
        self.config.commit()
        self.insert_source_eco_map(data)

    def get_eco_ids(self):
        """Select and map eco_ids to their bubble id's"""
        eco_map = {}
        result_query = self.curr.execute(
            f"""SELECT  eco_name,eco_id
                        from ecosystem
                        """).fetchall()

        for i in result_query:
            eco_name = i[0]

            eco_id = i[1]
            eco_map[eco_name] = eco_id

        return eco_map
    def insert_source_eco_map(self,data):
        eco_ids = data['eco_ids']
        source_id = data['source_id']
        already_exists = self.curr.execute(f"""Select eco_id from  serving
                                    where source_id = %s""", (source_id)).fetchall()
        already_exists = Counter([i[0] for i in already_exists])

        for e in eco_ids:
            if already_exists[e]:
                continue

            self.curr.execute(
                f""" insert into serving (eco_id,source_id) value (%s,%s) """,
                (
                    e,
                    source_id
                ))

        self.config.commit()

    def insert_ecosystem(self,eco_data:dict):
        name = eco_data['name']
        prod_id = eco_data['bubble_id']
        location = eco_data['location']
        result_query = self.db_connector.curr.execute(
            f"""SELECT  eco_id, production_id
                                from ecosystem
                                """).fetchall()
        pass
    def add_batch(self, source_id:int,status:int)->int:
        """Create new batch record for each source extracted"""
        batch_query = self.curr.execute(f"""SELECT batch_id from batch
                                        where source_id= {source_id} and 
                                        distributed = 0  
                                        """)


        batch_id = batch_query.fetchone()
        if batch_id:
            #if the undistributed batch record already exists for a source, return batch_id
            return batch_id[0]

        batch_query = self.curr.execute(
            f""" insert into batch (date_added,distributed,source_id,status) value (%s,%s, %s,%s) """,
            (
                f'{self.today}',
                0,
                source_id,
                status
            ))

        self.config.commit()
        return batch_query.lastrowid


    def update_label(self, source_id:int,label:str)->int:

        """Create new batch record for each source extracted"""
        if not label:
            self.curr.execute(f""" update supply set target_block= done=1,needs_processing=0  where source_id = {source_id}""")

        self.curr.execute(f""" update supply set target_block= '{label}' where source_id = {source_id}""")
        
        self.config.commit()
        return  0
    def get_sources(self):
        sources = self.curr.execute(f"""Select supply.source_id, asset, url,base_url,target_block
         from  supply
         inner join supplier on supplier.supplier_id = supply.supplier_id
                                    where live=0 and needs_processing=1 and done=0""").fetchall()
        data = []
        keys = ['source_id', 'asset', 'url','base_url','target_block']
        for i in sources:
            data.append(self.make_dataset(keys,i))
        random.shuffle(data)
        return data[:60]


