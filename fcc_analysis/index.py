from datetime import datetime
import io
import itertools
import json
import math
import time
import warnings
import multiprocessing
from tqdm import tqdm
import glob

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
        cnt = 0
        files = glob.glob("/home/shane/fcc-comment-analysis/data/*.json")
        print('FILES', len(files))
        total = len(files)
        progress = tqdm(total=total)
        for idx, fname in enumerate(files):
            with open(fname) as data_file:
                data = json.load(data_file)
                self.bulk_index_noq(data['filings'])
                progress.update(1)
        progress.close()

    def bulk_index_noq(self, documents):
        endpoint = '{}{}/filing/{}'.format(
            self.endpoint,
            'fcc-comments',
            '_bulk'
        )
        payload = io.StringIO()
        payload_size = 0
        created = False

        headers = headers = {'Content-type': 'application/x-ndjson', 'Accept': 'text/plain'}
        
        for document in documents:
            #document = queue.get()
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
            #print('JSON', response.json())
            for item in response.json()['items']:
                if item['create']['status'] == 201:
                    created = True

        return created
