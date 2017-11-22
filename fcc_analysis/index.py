from datetime import datetime
import io
import itertools
import json
import math
import time
import warnings
import multiprocessing
from tqdm import tqdm

import requests


class CommentIndexer:

    def __init__(self, lte=None, gte=None, limit=250, sort='date_disseminated,DESC', fastout=False, verify=True, endpoint='http://127.0.0.1/'):
        if gte and not lte:
            lte = datetime.now().isoformat()
        if lte and not gte:
            gte = '2000-01-01'
        self.lte = lte
        self.gte = gte
        self.limit = limit
        self.sort = sort
        self.fastout = fastout
        self.verify = verify
        self.endpoint = endpoint
        self.fcc_endpoint = 'https://ecfsapi.fcc.gov/filings'

    def run(self):
        index_queue = multiprocessing.Queue()

        bulk_index_process = multiprocessing.Process(
            target=self.bulk_index, args=(index_queue,),
        )
        bulk_index_process.start()
        total = self.get_total()
        if not total:
            print('error loading document total; using estimate')
            total = 5000000
        progress = tqdm(total=total)

        #for comment in self.iter_comments():
        with open('/home/shane/fcc-comment-analysis/data/2918000.json') as data_file:    
                data = json.load(data_file)
                for comment in data['filings']:
                    index_queue.put(comment)
                    progress.update(1)

        index_queue.put(None)
        bulk_index_process.join()
        progress.close()

    def build_query(self):
        query = {
            'proceedings.name': '17-108',
            'sort': self.sort
        }
        if self.lte and self.gte:
            query['date_received'] = '[gte]{gte}[lte]{lte}'.format(
                gte=self.gte,
                lte=self.lte
            )
        return query

    def get_total(self):
        query = self.build_query()
        query['limit'] = 1
        response = requests.get(self.fcc_endpoint, params=query)
        try:
            agg = response.json().get('aggregations', {})
            if not agg:
                return None
            for bucket in agg.get('proceedings_name', {}).get('buckets', []):
                if bucket['key'] == query['proceedings.name']:
                    return bucket['doc_count']
        except json.decoder.JSONDecodeError:
            return None
        return None


    def iter_comments(self, page=0):
        query = self.build_query()
        for page in itertools.count(0):
            query.update({
                'limit': self.limit,
                'offset': page * self.limit,
            })
            for i in range(7):
                response = requests.get(self.fcc_endpoint, params=query)

                try:
                    filings = response.json().get('filings', [])
                except json.decoder.JSONDecodeError:
                    # Exponentially wait--sometimes the API goes down.
                    time.sleep(math.pow(2, i))
                    continue
                else:
                    break
            for filing in filings:
                yield filing

            if len(filings) != self.limit:
                break

    def bulk_index(self, queue):
        endpoint = '{}{}/filing/{}'.format(
            self.endpoint,
            'fcc-comments',
            '_bulk'
        )
        print('ENDPOINT', endpoint)
        payload = io.StringIO()
        payload_size = 0
        created = False

        headers = headers = {'Content-type': 'application/x-ndjson', 'Accept': 'text/plain'}
        
        while True:
            document = queue.get()
            if document is None:
                break

            try:
                del document['_index']
            except KeyError:
                pass

            index = {"create": {"_id": document['id_submission']}}
            payload_size += payload.write(json.dumps(index))
            payload_size += payload.write('\n')
            payload_size += payload.write(json.dumps(document))
            payload_size += payload.write('\n')

            if payload_size > 8 * 1024 * 1024:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    #response = requests.post(endpoint, data=payload.getvalue(), verify=self.verify)
                    response = requests.post(endpoint, data=payload.getvalue(), verify=self.verify, headers=headers)
                    if response == 413:
                        raise Exception('Too large!')
                    payload = io.StringIO()
                    payload_size = 0
                    for item in response.json()['items']:
                        if item['create']['status'] == 201:
                            created = True

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            response = requests.post(endpoint, data=payload.getvalue(), verify=self.verify, headers=headers)
            payload = io.StringIO()
            payload_size = 0
            print('JSON', response.json())
            for item in response.json()['items']:
                if item['create']['status'] == 201:
                    created = True

        return created
