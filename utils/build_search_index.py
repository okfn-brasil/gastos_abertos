# -*- coding: utf-8 -*-

''' Build the search indexes.

Usage:
    ./build_search_index [RESOURCE]
    ./build_search_index (-h | --help)

Options:
    -h --help   Show this message.
'''

import os

import pandas as pd
from concurrent import futures

from gastosabertos.contratos.models import Contrato
from utils import ProgressCounter


def build_contratos_index():
    print("Building Contratos index")
    Contrato.build_search_index()


def download_contratos_files(csv_file='../data/urls.csv', directory='../data/contratos'):
    if not os.path.exists(directory):
        os.makedirs(directory)

    def download_and_save(url, directory):
        if not isinstance(url, basestring):
            return

        import os.path
        import urllib2

        filename = url.split('/')[-1]
        path = os.path.join(directory, filename)
        if not os.path.isfile(path):
            file = urllib2.urlopen(url)
            content = file.read()
            with open(path,'w') as f:
                f.write(content)

    print("Downloading Contratos files from: {}".format(csv_file))
    data = pd.read_csv(csv_file)

    total = len(data)
    counter = ProgressCounter(total)

    downloaded = 0
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = dict((executor.submit(download_and_save, d['file_txt'], directory), d['file_txt'])
                             for di, d in data.iterrows())

        for future in futures.as_completed(future_to_url):
            url = future_to_url[future]
            counter.update(1)
            downloaded += 1
            if future.exception() is not None:
                print('%r generated an exception: %s' % (url,
                                                         future.exception()))

    counter.end()

    print("Downloaded {} Contratos".format(downloaded))


if __name__ == '__main__':
    pass
