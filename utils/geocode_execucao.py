#!/usr/bin/env python
# coding: utf-8

''' Geocodes the execucao table.

Usage:
    ./geocode_execucao [DATA_FOLDER] [TERMS_FOLDER] [LINES_PER_INSERT]

Options:
    -h --help   Show this message.
'''

from __future__ import unicode_literals  # unicode by default

from docopt import docopt

from gastosabertos.execucao.models import Execucao
from utils import ProgressCounter, get_db
from geocoder import Geocoder
from update_execucao_year_info import update_all_years_info


TYPE_STR = type("")


def get_geolocable_cells(row):
    # Returns all cells that are strings.
    # This avoids trying to geolocate ints or floats.
    # But maybe not all string columns should be used...
    return [i for i in row.data.values() if type(i) == TYPE_STR]


def geocode_all(db, data_folder="geocoder/data",
                terms_folder="geocoder/terms",
                lines_per_insert=1000):

    print("Loading table...")
    # The query bellow seems not very efficient...
    # Maybe change it as the link says.
    # https://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
    non_geocoded = Execucao.query.filter(Execucao.searched == False).all()
    with Geocoder(data_folder, terms_folder) as geocoder:
        counter = ProgressCounter(len(non_geocoded), print_abs=True)
        to_be_inserted = 0
        for row in non_geocoded:
            cells = get_geolocable_cells(row)
            geoent = geocoder.geocode_list(cells)
            if geoent:
                lat, lon, reg = geoent.best_coords()
                if lat:
                    row.point = "POINT(%s %s)" % (lon, lat)
            row.searched = True
            to_be_inserted += 1
            if to_be_inserted == lines_per_insert:
                db.session.commit()
                to_be_inserted = 0
            counter.update()
        if to_be_inserted:
            db.session.commit()
        counter.end()


if __name__ == '__main__':
    db = get_db()

    arguments = docopt(__doc__)
    args = {}

    lines_per_insert = arguments['LINES_PER_INSERT']
    if lines_per_insert:
        args['lines_per_insert'] = int(lines_per_insert)

    for arg in ["DATA_FOLDER", "TERMS_FOLDER"]:
        folder = arguments[arg]
        if folder:
            args[arg.lower()] = folder

    geocode_all(db, **args)
    update_all_years_info(db)
