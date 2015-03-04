# -*- coding: utf-8 -*-

import os

from utils import make_dir


INSTANCE_FOLDER_PATH = '/tmp'


class BaseConfig(object):

    PROJECT = "gastos_abertos"

    # Get app root path, also can use flask.root_path.
    # ../../config.py
    PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    DEBUG = False
    TESTING = False

    # http://flask.pocoo.org/docs/quickstart/#sessions
    SECRET_KEY = 'secret key'

    LOG_FOLDER = os.path.join(INSTANCE_FOLDER_PATH, 'logs')
    make_dir(LOG_FOLDER)


class DefaultConfig(BaseConfig):

    DEBUG = True
    # Flask-babel: http://pythonhosted.org/Flask-Babel/
    ACCEPT_LANGUAGES = ['pt_BR']
    BABEL_DEFAULT_LOCALE = 'en'

    # Flask-Sqlalchemy: http://packages.python.org/Flask-SQLAlchemy/config.html
    SQLALCHEMY_ECHO = False
    # SQLITE for prototyping.
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + INSTANCE_FOLDER_PATH + '/db.sqlite'


class TestConfig(BaseConfig):
    TESTING = True
    WTF_CSRF_ENABLED = False
