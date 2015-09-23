#!/usr/bin/env python
# coding: utf-8

''' This script updates the table with metadata about each execucao year.
No arguments required.

Usage:
    ./update_execucao_year_info [options]

Options:
    -h --help   Show this message.
'''
from sqlalchemy import func
from docopt import docopt

from gastosabertos.execucao.models import Execucao, ExecucaoYearInfo
from utils import get_db


def create_year_info_json(db, year):
    '''Creates a json with information about an year.'''
    q_total = db.session.query(Execucao).filter(
        Execucao.get_year() == year)
    num_total = q_total.count()
    q_mapped = q_total.filter(Execucao.point_found())
    num_mapped = q_mapped.count()

    rows = {
        'total': num_total,
        'mapped': num_mapped,
        # TODO: calcular regionalizados...
        'region': num_mapped,
    }

    values = []
    fields = [
        ('orcado', 'sld_orcado_ano'),
        ('atualizado', 'vl_atualizado'),
        ('empenhado', 'vl_empenhadoliquido'),
        ('liquidado', 'vl_liquidado')
    ]
    for name, db_field in fields:
        q = (db.session.query(
            func.sum(Execucao.data[db_field].cast(db.Float)))
            .filter(Execucao.get_year() == year))

        total = q.scalar()
        mapped = q.filter(Execucao.point_found()).scalar()
        if mapped is None:
            mapped = 0
        values.append({
            'name': name,
            'total': total,
            'mapped': mapped,
            # TODO: calcular regionalizados...
            'region': mapped,
        })

    last_update = (db.session.query(Execucao.data['datafinal'])
                   .filter(Execucao.get_year()==year)
                   .distinct().all()[-1][0])

    return {
        'data': {
            'rows': rows,
            'values': values,
            'last_update': last_update if last_update else str(year),
        }
    }


def update_year_info(db, year):
    '''Updates information about an year.'''
    data = create_year_info_json(db, year)
    old_data = db.session.query(ExecucaoYearInfo).filter_by(year=year).first()
    if old_data:
        old_data.data = data
    else:
        db.session.add(ExecucaoYearInfo(year=year, data=data))
    db.session.commit()


def update_all_years_info(db):
    dbyears = db.session.query(Execucao.get_year()).distinct().all()
    for tup in dbyears:
        update_year_info(db, tup[0])


if __name__ == '__main__':
    db = get_db()
    arguments = docopt(__doc__)
    tables = [ExecucaoYearInfo.__table__]
    try:
        db.metadata.drop_all(db.engine, tables, checkfirst=True)
    except:
        pass
    db.metadata.create_all(db.engine, tables, checkfirst=True)
    update_all_years_info(db)
