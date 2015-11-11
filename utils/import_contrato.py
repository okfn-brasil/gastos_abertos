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
from docopt import docopt

from gastosabertos.contratos.models import Contrato
from utils import ProgressCounter, get_db


def parse_money(money_string):
    return str(money_string).replace('.', '').replace(',', '.')


def parse_date(date_string):
    new_date = datetime.strptime(date_string, '%d/%m/%Y')
    return new_date


def insert_rows(db, rows_data):
    ins = insert(Contrato.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


def insert_all(db, csv_file='../data/contratos-2014.xls', lines_per_insert=100):
    print("Importing Contratos from: {}".format(csv_file))
    data = pd.read_excel(csv_file)
    data = data.fillna(-1)

    cache = {}
    to_insert = []
    total = len(data)
    inserted = 0
    counter = ProgressCounter(total)

    for row_i, row in data.iterrows():

        r = {}

        if len(to_insert) == lines_per_insert:
            inserted += len(to_insert)
            insert_rows(db, to_insert)
            to_insert = []
            # Progress counter
            counter.update(lines_per_insert)

        r['numero'] = int(row_i) + 1
        r['orgao'] = row['Orgao']
        r['data_assinatura'] = parse_date(row['Data da Assinatura'])
        r['vigencia'] = int(row['Vigencia']) if not np.isnan(row['Vigencia']) else -1
        r['objeto'] = row['Objeto']
        r['modalidade'] = row['Modalidade']
        r['evento'] = row['Evento']
        r['processo_administrativo'] = row['Processo Administrativo']
        r['cnpj'] = row['CNPJ']
        r['nome_fornecedor'] = row['Nome']
        r['valor'] = parse_money(row['Valor'])
        r['licitacao'] = row['Licitacao\n']
        r['data_publicacao'] = parse_date(row['Data Publicacao'])

        to_insert.append(r)

    if len(to_insert) > 0:
        inserted += len(to_insert)
        insert_rows(db, to_insert)

    counter.end()

    print("Imported {} Contratos".format(inserted))


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
