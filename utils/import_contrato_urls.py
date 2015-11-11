# -*- coding: utf-8 -*-

''' Read a xls with Contracts and insert them in the DB.

Usage:
    ./import_contrato [FILE] [LINES_PER_INSERT]
    ./import_contrato (-h | --help)

Options:
    -h --help   Show this message.
'''
import pandas as pd
from sqlalchemy import update
from docopt import docopt

from gastosabertos import create_app

from gastosabertos.contratos.models import Contrato

from utils import ProgressCounter


def get_db():
    from gastosabertos.extensions import db
    app = create_app()
    db.app = app
    return db


def insert_all(db, csv_file='../data/urls.csv', lines_per_insert=100):
    print("Getting Contratos urls from: " + csv_file)
    data = pd.read_csv(csv_file)

    total = len(data)
    counter = ProgressCounter(total)

    to_update = 0
    updated = 0
    for di, d in data.iterrows():
        to_update += 1
        stmt = update(Contrato).values({'file_url':d['file_url'], 'txt_file_url':d['file_txt']}).where(Contrato.numero == str(d['numero']))
        db.session.execute(stmt)
        if to_update == lines_per_insert or (updated + to_update) == total:
            counter.update(to_update)
            updated += to_update
            to_update = 0
            db.session.commit()

    counter.end()

    print("Updated {} Contratos".format(updated))


if __name__ == '__main__':
    arguments = docopt(__doc__)
    args = {}
    csv_file = arguments['FILE']
    if csv_file:
        args['csv_file'] = csv_file
    lines_per_insert = arguments['LINES_PER_INSERT']
    if lines_per_insert:
        args['lines_per_insert'] = int(lines_per_insert)
    db = get_db()
    insert_all(db, **args)
