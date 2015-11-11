# -*- coding: utf-8 -*-

from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.restplus import Api

db = SQLAlchemy()
api = Api(version='1.0',
          title='Gastos Abertos',
          description='API para acesso a dados orçamentários',
          default='/api/v1')
