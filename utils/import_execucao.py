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

from gastosabertos import create_app
from gastosabertos.execucao.models import Execucao
from utils import ProgressCounter

# import geocoder


def get_db():
    from gastosabertos.extensions import db
    app = create_app()
    db.app = app
    return db


# def parse_money(money_string):
#     if money_string[0] == '-':
#         return -float(money_string[3:].replace('.', '').replace(',', '.'))
#     else:
#         return float(money_string[3:].replace('.', '').replace(',', '.'))


# def parse_date(date_string):
#     year_month = datetime.strptime(date_string, '%Y-%m')
#     date = year_month + timedelta(
#         days=calendar.monthrange(
#             year_month.year, year_month.month
#         )[1] - 1)
#     return date


# def parse_code(code_string):
#     return [int(i) for i in code_string.split('.')]


def insert_rows(db, rows_data):
    # import IPython; IPython.embed(); exit
    ins = insert(Execucao.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


# def insert_all(db, csv_file='../data/receitas_min.csv', lines_per_insert=100):
#     data = pd.read_csv(csv_file, encoding='utf8')

#     cache = {}
#     to_insert = []
#     total_lines = len(data)
#     current_line = 0.0

#     for row_i, row in data.iterrows():
#         current_line += 1

#         r = {}

#         if len(to_insert) == lines_per_insert:
#             insert_rows(db, to_insert)
#             to_insert = []
#             # Progress counter
#             print(str(int(current_line/total_lines*100))+'%')

#         r['original_code'] = row['codigo']
#         r['description'] = row['descricao']
#         r['date'] = parse_date(row['data'])
#         r['monthly_outcome'] = parse_money(row['realizado_mensal'])
#         r['monthly_predicted'] = parse_money(row['previsto_mensal'])
#         code_parsed = parse_code(row['codigo'])
#         r['economical_category'] = code_parsed[0]

#         # Insert code reference
#         code_parts = map(int, r['original_code'].split('.'))
#         len_cp = len(code_parts)

#         for i in range(len_cp):
#             code = '.'.join(map(str, code_parts[:len_cp - i]))
#             if code not in cache:
#                 code_result = db.session.query(RevenueCode.id).filter(
#                     RevenueCode.code == code).all()
#                 if code_result:
#                     cache[code] = code_result[0][0]
#                     r['code_id'] = code_result[0][0]
#                     break
#             else:
#                 r['code_id'] = cache[code]
#                 break
#         else:
#             r['code_id'] = None

#         if len(code_parsed) >= 2:
#             r['economical_subcategory'] = code_parsed[1]
#         else:
#             r['economical_subcategory'] = None

#         if len(code_parsed) >= 3:
#             r['source'] = code_parsed[2]
#         else:
#             r['source'] = None

#         if len(code_parsed) >= 4:
#             r['rubric'] = code_parsed[3]
#         else:
#             r['rubric'] = None

#         if len(code_parsed) >= 5:
#             r['paragraph'] = code_parsed[4]
#         else:
#             r['paragraph'] = None

#         if len(code_parsed) == 6:
#             r['subparagraph'] = code_parsed[5]
#         else:
#             r['subparagraph'] = None

#         to_insert.append(r)

#     if len(to_insert) > 0:
#         insert_rows(db, to_insert)

def insert_csv(csv, lines_per_insert=100):
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


if __name__ == '__main__':
    db = get_db()

    arguments = docopt(__doc__)
    args = {}
    lines_per_insert = arguments['LINES_PER_INSERT']
    if lines_per_insert:
        args['lines_per_insert'] = int(lines_per_insert)

    folder = arguments['FOLDER']
    if not folder:
        folder = "../../gastos_abertos_dados/Orcamento/execucao/"

    csvs = sorted([i for i in os.listdir(folder) if i[-4:] == ".csv"])

    for csv in csvs:
        insert_csv(os.path.join(folder, csv), **args)

        # print(table)
        # table = table#.iloc[0:100]
        # print("Adding pks")
        # table = geocoder.add_pks(table)
        # print("Adding geos")
        # table = geocoder.add_geos(table)
        # return table
