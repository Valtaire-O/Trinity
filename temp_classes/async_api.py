import json
from collections import Counter
import asyncio
from time import perf_counter
import time
import aiohttp
import io
from .base_query import CloudConnect

class Asnyc_API():
    def __init__(self):
        self.response_map = Counter()
        self.cloud_conn = CloudConnect()
        self.scrape_key = self.cloud_conn.get_scraping_api_key()

    async def post(self, s, data, endpoint, headers):
        try:
               body = data['body']
               async with s.post(endpoint, headers=headers, json=body, ssl=False) as r:
                   text = await r.json()

                   print(f"Context Post Status: {r.status}")
                   return {"status": r.status, 'response': text}
        except :
               print(f"Context Post Status: 500")
               return {"status": 500, 'response': None}

    async def fetch(self, s, data, endpoint):
        try:
            async with s.get(endpoint, ssl=False) as r:
                print(f"Scraping Status: {r.status}")
                self.response_map[endpoint] = {"status": r.status}
                html = await r.json()
                if '&screenshot' in endpoint:

                    return {'html': html['result']['content'], 'status': r.status,
                            'screenshots':html['result']["screenshots"]}

                return {'html': html['result']['content'], 'status': r.status}
        except:
            print(f"Scraping Status: 504")
            return {'response': '', 'status': 504, 'html':''}

    async def fetch_img(self,s,data, endpoint):
       async with s.get(endpoint, ssl=False) as r:
           text = await r.content.read()
           byte_obj = io.BytesIO(text)
           return  {"status":r.status, 'response':byte_obj}



    async def handle_all(self, s, data,  headers, scrape_state, image_state, api_state):
        tasks = []

        for d in data:
            endpoint = d['url']

            if api_state:
                task = asyncio.create_task(self.post(s, d, endpoint, headers))
            elif scrape_state:
                task = asyncio.create_task(self.fetch(s, d, endpoint))
            elif image_state:
                task = asyncio.create_task(self.fetch_img(s, d, endpoint))

            else:
                continue

            if task:
                tasks.append(task)

        res = await asyncio.gather(*tasks)
        return res

    async def start_async_http(self, data, headers, scrape_state=False, image_state=False, api_state=False,limit_per_host=20,base_limit=40):

        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=90, sock_read=90)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=base_limit,limit_per_host=limit_per_host) ,timeout=session_timeout) as session:
            html = await self.handle_all(session, data, headers, scrape_state, image_state, api_state)
            i = -1
            for d in data:
                i += 1
                if scrape_state is True:

                    d['response'] = html[i]
                elif api_state is True or image_state is True:
                    d['response'] =  html[i]["response"]
                    d['status'] = html[i]["status"]

        return data

    def make_calls(self,data,headers,scrape_state=False,image_state=False,api_state=False,sleep_time=2,limit_per_host=20,base_limit=50):
        #confirm at least one state has been provided
        check_state = [i for i in [scrape_state, image_state, api_state] if i == True]
        if len(check_state) ==0 or len(check_state)>1:
            raise AssertionError

        req_data = []
        count = 0
        '''Batch up dataset for async requests'''
        batch_data = self.make_batch(data,50)
        n = len(batch_data)
        start = perf_counter()

        for dim in batch_data:
            count += 1
            print(f'Request for batch... {count} out of {n} batches')

            extract = asyncio.run(Asnyc_API().start_async_http(dim, headers=headers,
                                                               scrape_state=scrape_state, image_state=image_state, api_state=api_state,
                                                               limit_per_host=limit_per_host,base_limit=base_limit))
            req_data.extend(extract)
            time.sleep(sleep_time)
        stop = perf_counter()
        print(f"finished in {stop - start}seconds")
        return req_data

    def make_batch(self,arr, batch_size):
        """make a matrix from a flat list"""
        i = 0
        j = batch_size
        all_data = []
        n = len(arr)
        if batch_size > n:
            return [arr]
        while j <= n:
            if j == n:
                j += 1

            all_data.append(arr[i:j])
            remaining = len(arr[j:])
            if 0 < remaining < batch_size:
                # print(f" Rest of slice {test[j:]}")
                i += batch_size
                j += remaining
                batch_size = remaining
                continue

            i += batch_size
            j += batch_size
        # print(f"'result: {all_data}")
        return all_data

    def api_key(self):
        key = '&key=' +self.scrape_key

        return key
    def api_params(self):
        params = '&country=us&render_js=true&rendering_wait=2500&proxy_pool=public_residential_pool'
        return params

    def api_url(self):
        return 'https://api.scrapfly.io/scrape?url='



