# -*- coding: utf-8 -*-

''' Read a CSV with revenues and insert them in the DB.

Usage:
    ./import_revenue [FILE] [LINES_PER_INSERT]
    ./import_revenue (-h | --help)

Options:
    -h --help   Show this message.
'''
import pandas as pd
from datetime import datetime, timedelta
import calendar
from sqlalchemy.sql.expression import insert
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
#        return -float(money_string[3:].replace('.', '').replace(',', '.'))
#    else:
#        return float(money_string[3:].replace('.', '').replace(',', '.'))


def parse_date(date_string):
    new_date = datetime.strptime(date_string, '%d/%m/%Y')
    return new_date


def parse_code(code_string):
    return [int(i) for i in code_string.split('.')]


def insert_rows(db, rows_data):
    ins = insert(Contrato.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


def insert_all(db, csv_file='../data/contratos-2014.xls', lines_per_insert=100):
    data = pd.read_excel(csv_file)

    cache = {}
    to_insert = []
    total_lines = len(data)
    current_line = 0.0

    for row_i, row in data.iterrows():
        current_line += 1

        r = {}

        if len(to_insert) == lines_per_insert:
            insert_rows(db, to_insert)
            to_insert = []
            # Progress counter
            print(str(int(current_line/total_lines*100))+'%')

        r['numero'] = row_i + 1
        r['orgao'] = row['Orgao']
        r['data_assinatura'] = parse_date(row['Data da Assinatura'])
        r['vigencia'] = row['Vigencia']
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
        insert_rows(db, to_insert)


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
