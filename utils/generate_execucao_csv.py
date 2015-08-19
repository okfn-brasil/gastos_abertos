#!/usr/bin/env python
# coding: utf-8

''' Generate a CSV for year with execucao data.

Usage: generate_execucao_csv [options] [<year>...]

Options:
-h, --help                        Show this message.
-o, --outfolder <outfolder>       Folder where to place files.
                                  Maybe relative.
                                  [default: current folder]
'''

from __future__ import unicode_literals  # unicode by default
import os
import json
import csv

from docopt import docopt

from gastosabertos.execucao.models import Execucao
from utils import get_db

# For Python 2...
if 'unix' not in csv.list_dialects():
    csv.register_dialect('unix', lineterminator='\n')


def get_lonlat(geo):
    if not geo:
        return ('', '')
    else:
        return json.loads(geo)['coordinates']


def generate_year(db, year, outfolder):
    year_data = db.session.query(
        Execucao, Execucao.point.ST_AsGeoJSON(3)).filter(
            Execucao.get_year() == year).all()

    rows = []
    for row, geo in year_data:
        data = {
            'estado': row.state,
            'codigo': row.code,
        }

        # Add row.data fields taking care of unicode
        for k, v in row.data.items():
            try:
                data[k] = v.encode('utf-8')
            except AttributeError:
                # For non strings types
                data[k] = v

        lon, lat = get_lonlat(geo)
        data['longitude'] = lon
        data['latitude'] = lat
        rows.append(data)

    # Sort by code
    rows.sort(lambda x, y: x['codigo'] < y['codigo'])

    filepath = os.path.join(outfolder, year + '.csv')
    with open(filepath, 'w') as outfile:
        writer = csv.DictWriter(outfile,
                                fieldnames=sorted(rows[0].keys()),
                                dialect='unix')
        writer.writeheader()
        writer.writerows(rows)


def generate_all(db, outfolder, years):
    # if years is an empty list, calculate for all years in the DB
    if not years:
        dbyears = db.session.query(Execucao.get_year()).distinct().all()
        years = sorted([str(i[0]) for i in dbyears])

    for year in years:
        print(year)
        generate_year(db, year, outfolder)


if __name__ == '__main__':
    db = get_db()

    arguments = docopt(__doc__)

    outfolder = arguments['--outfolder']
    if outfolder == "current folder":
        outfolder = os.getcwd()

    generate_all(db, outfolder, arguments['<year>'])
