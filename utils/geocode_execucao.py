#!/usr/bin/env python
# coding: utf-8

# ''' Read a CSV with execucao and insert it in the DB.

# Usage:
#     ./import_execucao [FOLDER] [LINES_PER_INSERT]
#     ./import_execucao (-h | --help)

# Options:
#     -h --help   Show this message.
# '''

from __future__ import unicode_literals  # unicode by default

from gastosabertos.execucao.models import Execucao
from utils import ProgressCounter, get_db
from geocoder import Geocoder


TYPE_STR = type("")


def get_geolocable_cells(row):
    return [i for i in row.data.values() if type(i) == TYPE_STR]


def geocode_all(db, data_folder="geocoder/data",
                terms_folder="geocoder/terms"):

    # The query bellow seems not very efficient...
    # Maybe change it as the link says.
    # https://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
    non_geocoded = Execucao.query.filter(Execucao.geo == None).all()
    with Geocoder(data_folder, terms_folder) as geocoder:
        counter = ProgressCounter(len(non_geocoded), print_abs=True)
        for row in non_geocoded:
            cells = get_geolocable_cells(row)
            geoent = geocoder.geocode_list(cells)
            if geoent:
                print(geoent.best_coords())
            counter.update()
        counter.end()


if __name__ == '__main__':
    db = get_db()
    geocode_all(db)


    # arguments = docopt(__doc__)
    # args = {}
    # lines_per_insert = arguments['LINES_PER_INSERT']
    # if lines_per_insert:
    #     args['lines_per_insert'] = int(lines_per_insert)

    # folder = arguments['FOLDER']
    # if not folder:
    #     folder = "../../gastos_abertos_dados/Orcamento/execucao/"

    # csvs = sorted([i for i in os.listdir(folder) if i[-4:] == ".csv"])

    # for csv in csvs:
    #     insert_csv(os.path.join(folder, csv), **args)
