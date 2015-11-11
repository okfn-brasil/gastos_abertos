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
@manager.option('-h', '--host', help='Host')
def run(host='127.0.0.1'):
    """Run in local machine."""
    manager.app.run(host=host)


@manager.command
def test():
    """Run tests."""
    return


@manager.command
def initdb():
    """Init or reset database"""

    db.drop_all()
    db.create_all()


def _importrevenue():
    """Import revenue data to the database"""
    from utils.import_revenue_codes import import_codes
    from utils import import_revenue

    # Revenue
    import_codes(db)
    import_revenue.insert_all(db, csv_file='data/receitas_min.csv')
    # insert_all(db, csv_file='data/receitas_min.csv', lines_per_insert=80)


def _importcontratos():
    """Import contratos data to the database"""
    from utils import import_contrato, import_contrato_urls

    # Contratos
    import_contrato.insert_all(db, csv_file='data/contratos-2014.xls')
    import_contrato_urls.insert_all(db, csv_file='data/urls.csv')


def _importexecucao():
    """Import execucao data to the database"""
    from utils import (import_execucao, geocode_execucao,
                       update_execucao_year_info)

    # Execucao
    folder = '../gastos_abertos_dados/Orcamento/execucao/'
    import_execucao.insert_all(db, path=folder)
    data_folder = 'utils/geocoder/data'
    terms_folder = 'utils/geocoder/terms'
    geocode_execucao.geocode_all(db, data_folder, terms_folder)
    update_execucao_year_info.update_all_years_info(db)


@manager.command
@manager.option('-d', '--data', help='Data type to be imported')
@manager.option('-r', '--reset', help='Remove previous data from database before importing')
def importdata(data='all', reset=False):
    """Import the data to the database"""
    data = data.lower()

    if reset:
        initdb()

    if data in ('all', 'revenue'):
        _importrevenue()
    if data in ('all', 'contratos'):
        _importcontratos()
    if data in ('all', 'execucao'):
        _importexecucao()


@manager.command
@manager.option('-r', '--resource', help='Resource to be indexed')
def build_search_index(resource='all'):
    """Build search index"""
    from utils import build_search_index
    resource = resource.lower()

    if resource in ('all', 'contratos'):
        build_search_index.build_contratos_index()


@manager.command
def download_contratos():
    """Download Contratos files"""
    from utils import build_search_index

    build_search_index.download_contratos_files(csv_file='data/urls.csv', directory='data/contratos')


if __name__ == "__main__":
    manager.run()
