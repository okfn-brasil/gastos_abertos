# -*- coding: utf-8 -*-

''' Read a xls with Contracts and insert them in the DB.

Usage:
    ./import_contrato [FILE] [LINES_PER_INSERT]
    ./import_contrato (-h | --help)

Options:
    -h --help   Show this message.
'''
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
from sqlalchemy.sql.expression import insert
from sqlalchemy import update
from docopt import docopt

from gastosabertos import create_app

from gastosabertos.contratos.models import Contrato 


def get_db():
    from gastosabertos.extensions import db
    app = create_app()
    db.app = app
    return db


def parse_money(money_string):
    return str(money_string).replace('.', '').replace(',', '.')


def parse_date(date_string):
    new_date = datetime.strptime(date_string, '%d/%m/%Y')
    return new_date


def insert_rows(db, rows_data):
    ins = insert(Contrato.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


def insert_all(db, csv_file='../data/urls.csv', lines_per_insert=100):
    data = pd.read_csv(csv_file)

    for di, d in data[:10].iterrows():
        stmt = update(Contrato).values({'file_url':d['file_url'], 'txt_file_url':d['file_txt']}).where(Contrato.numero == d['numero'])		
        db.session.execute(stmt)
        db.session.commit()

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
