# -*- coding: utf-8 -*-

from flask.ext.script import Manager

from gastosabertos.extensions import db
from gastosabertos import create_app


manager = Manager(create_app)
manager.add_option('-i', '--inst', dest='instance_folder', required=False)


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
    from utils.import_revenue import insert_all
    import_codes(db)
    insert_all(db, csv_file='data/receitas_min.csv', lines_per_insert=80)


if __name__ == "__main__":
    manager.run()
