# -*- coding: utf-8 -*-

from flask.ext.script import Manager, Shell

from gastosabertos.extensions import db
from gastosabertos import create_app


manager = Manager(create_app)
manager.add_option('-i', '--inst', dest='instance_folder', required=False)
manager.add_command('shell', Shell(make_context=lambda: {
    'app': manager.app,
    'db': db,
}))


@manager.command
def run():
    """Run in local machine."""
    manager.app.run()


@manager.command
def test():
    """Run tests."""
    return


@manager.command
def initdb():
    """Init or reset database"""

    db.drop_all()
    db.create_all()


@manager.command
def importdata():
    """Import the data to the database"""
    from utils.import_revenue_codes import import_codes
    from utils import (import_revenue, import_contrato,
                       import_execucao, geocode_execucao)

    # Revenue
    import_codes(db)
    import_revenue.insert_all(db, csv_file='data/receitas_min.csv')
    # insert_all(db, csv_file='data/receitas_min.csv', lines_per_insert=80)

    # Contratos
    import_contrato.insert_all(db, csv_file='data/contratos-2014.xls')

    # Execucao
    folder = '../gastos_abertos_dados/Orcamento/execucao/'
    import_execucao.insert_all(db, folder=folder)
    data_folder = 'utils/geocoder/data'
    terms_folder = 'utils/geocoder/terms'
    geocode_execucao.geocode_all(db, data_folder, terms_folder)


if __name__ == "__main__":
    manager.run()
