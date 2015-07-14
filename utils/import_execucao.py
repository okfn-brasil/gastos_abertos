#!/usr/bin/env python
# coding: utf-8

''' Read a CSV with execucao and insert it in the DB.

Usage:
    ./import_execucao [FOLDER] [LINES_PER_INSERT]
    ./import_execucao (-h | --help)

Options:
    -h --help   Show this message.
'''

from __future__ import unicode_literals  # unicode by default
import os

import pandas as pd
from docopt import docopt
from sqlalchemy.sql.expression import insert

from gastosabertos.execucao.models import Execucao
from utils import ProgressCounter, get_db


def insert_rows(db, rows_data):
    ins = insert(Execucao.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


def insert_csv(csv, lines_per_insert):
    print(csv)
    table = pd.read_csv(csv)
    counter = ProgressCounter(len(table))

    # ## Add code column ## #
    code_series = [col for name, col in table.iteritems()
                   if name[:3].lower() == "cd_"]
    # this column doesn't start with "cd_" but is a code
    code_series.append(table["projetoatividade"])
    # create table of codes
    code_table = pd.concat(code_series, axis=1)
    # create PK Series
    pks = pd.Series(['.'.join([str(i) for i in i[1][1:]])
                    for i in code_table.iterrows()],
                    name="code")
    # check pk uniqueness
    if pks.duplicated().values.sum() > 0:
        print("Warning: There are duplicated pks!")
    # add the pk series to the table
    table = pd.concat([table, pks], axis=1)
    # ## --------------- ## #

    to_insert = []

    for row_i, row in table.iterrows():

        if len(to_insert) == lines_per_insert:
            insert_rows(db, to_insert)
            to_insert = []
            # Progress counter
            counter.update(lines_per_insert)

        to_insert.append({"data": dict(row.iterkv())})

    if len(to_insert) > 0:
        insert_rows(db, to_insert)

    counter.end()


def insert_all(folder="../../gastos_abertos_dados/Orcamento/execucao/",
               lines_per_insert=100):

    csvs = sorted([i for i in os.listdir(folder) if i[-4:] == ".csv"])
    for csv in csvs:
        insert_csv(os.path.join(folder, csv), lines_per_insert)


if __name__ == '__main__':
    db = get_db()
    Execucao.metadata.create_all(db.engine, checkfirst=True)

    arguments = docopt(__doc__)
    args = {}

    lines_per_insert = arguments['LINES_PER_INSERT']
    if lines_per_insert:
        args['lines_per_insert'] = int(lines_per_insert)

    folder = arguments['FOLDER']
    if folder:
        args['folder'] = folder

    insert_all(**args)
