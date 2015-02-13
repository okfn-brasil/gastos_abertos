# -*- coding: utf-8 -*-

from flask.ext.script import Manager

from gastosabertos.extensions import db
from gastosabertos import create_app


app = create_app()
manager = Manager(app)


@manager.command
def run():
    """Run in local machine."""

    app.run()


@manager.command
def test():
    """Run tests."""

    return


@manager.command
def initdb():
    """Init or reset database"""

    db.drop_all()
    db.create_all()


if __name__ == "__main__":
    manager.run()
