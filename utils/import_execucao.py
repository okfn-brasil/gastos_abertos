#!/usr/bin/env python
# coding: utf-8

''' Read a CSV with execucao and insert it in the DB.

Usage:
    ./import_execucao [options] [PATH] [LINES_PER_INSERT]

PATH: Can be a CSV file or a folder. If it is a folder, insert all CSVs there.

Options:
    -h --help   Show this message.
    -D          Drop table.
'''

from __future__ import unicode_literals  # unicode by default
import os

import pandas as pd
from docopt import docopt
from sqlalchemy.sql.expression import insert

from gastosabertos.execucao.models import Execucao
from utils import ProgressCounter, get_db


def identify_state(data):
    if data['vl_liquidado']:
        return 'liquidado'
    elif data['vl_empenhadoliquido'] or data.get('vl_empenhado'):
        return 'empenhado'
    elif data['vl_atualizado']:
        return 'atualizado'
    else:
        return 'orcado'


def identify_capcor(data):
    '''Identify if it is a 'capital' or 'corrente' expense.'''
    num = data.get('categoria_despesa')
    if not num:
        num = str(data.get('cd_despesa'))[0]
    num = int(num)
    if num == 3:
        return 'corrente'
    elif num == 4:
        return 'capital'


def generate_code(row):
    '''Returns a code for the row. Hopefully it is unique.'''
    # Supress 'cd_exercicio' if is equals to 'cd_anoexecucao'
    if int(row['cd_anoexecucao']) == int(row['cd_exercicio']):
        return '.'.join([str(int(v)) for k, v in row.iterkv()
                         if k != 'cd_exercicio'])
    else:
        return '.'.join([str(int(v)) for v in row])


def insert_rows(db, rows_data):
    ins = insert(Execucao.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


def create_pks(table):
    code_series = [col for name, col in table.iteritems()
                   if name[:3].lower() == 'cd_']
    # this column doesn't start with 'cd_' but is a code
    code_series.append(table['projetoatividade'])
    # create table of codes
    code_table = pd.concat(code_series, axis=1)
    # create PK Series
    pks = pd.Series([generate_code(row[1]) for row in code_table.iterrows()],
                    name='code')

    # check pk uniqueness
    if pks.duplicated().values.sum() > 0:
        print('Warning: There are duplicated pks!')

    return pks


def prepare_row(code, row):
    data = dict(row.iterkv())
    return {
        'code': code,
        'data': data,
        'state': identify_state(data),
        'cap_cor': identify_capcor(data),
    }


def insert_csv(db, csv, lines_per_insert):
    table = pd.read_csv(csv)
    pks = create_pks(table)
    # db = get_db()
    counter = ProgressCounter(len(table))

    to_insert = []
    for row_i, row in table.iterrows():
        if len(to_insert) == lines_per_insert:
            insert_rows(db, to_insert)
            to_insert = []
            # Progress counter
            counter.update(lines_per_insert)
        to_insert.append(prepare_row(pks.iloc[row_i], row))

    if len(to_insert) > 0:
        insert_rows(db, to_insert)

    counter.end()


def insert_all(db, path='../../gastos_abertos_dados/Orcamento/execucao/',
               lines_per_insert=100):

    if os.path.isdir(path):
        csvs = sorted([i for i in os.listdir(path) if i[-4:] == '.csv'])
        for csv in csvs:
            print(csv)
            insert_csv(db, os.path.join(path, csv), lines_per_insert)
    else:
        # If path is not a folder, it should be a CSV file
        insert_csv(db, path, lines_per_insert)


def update_from_csv(db, csv):
    '''Update table using values from CSV. Slower than 'insert_csv' but raises
    no error if primary key already exists (just updates values).'''
    table = pd.read_csv(csv)
    pks = create_pks(table)
    counter = ProgressCounter(len(table))
    for row_i, row in table.iterrows():
        code = pks.iloc[row_i]
        row_model = db.session.query(Execucao).filter_by(code=code).first()
        new_row = prepare_row(code, row)
        if row_model:
            for key, new_value in new_row.iteritems():
                setattr(row_model, key, new_value)
                # old_value = getattr(row_model, key)
                # if old_value != new_value:
                #     print(key, old_value, new_value)
                #     setattr(row_model, key, new_value)
        else:
            db.session.add(Execucao(**new_row))
        counter.update()
    counter.end()
    db.session.commit()


if __name__ == '__main__':
    db = get_db()

    arguments = docopt(__doc__)
    args = {'db': db}

    if arguments['-D']:
        Execucao.metadata.drop_all(db.engine, checkfirst=True)

    Execucao.metadata.create_all(db.engine, checkfirst=True)

    lines_per_insert = arguments['LINES_PER_INSERT']
    if lines_per_insert:
        args['lines_per_insert'] = int(lines_per_insert)

    path = arguments['PATH']
    if path:
        args['path'] = path

    insert_all(**args)
