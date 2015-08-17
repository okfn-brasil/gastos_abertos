# -*- coding: utf-8 -*-

from sqlalchemy import Column
from sqlalchemy.dialects import postgresql
from geoalchemy2 import Geometry

from ..extensions import db


class Execucao(db.Model):

    __tablename__ = 'execucao'

    # id = Column(db.Integer, primary_key=True)
    code = Column(db.String(100), primary_key=True)
    # Official data
    data = Column(postgresql.JSONB)
    # Geodata about this expense line
    point = Column(Geometry('POINT'), default=None)
    # If an attempt were made to gecode this row
    searched = Column(db.Boolean, default=False)

    # Current state of this expense:
    # _states_names = {
    #     0: 'orcado',
    #     1: 'atualizado',
    #     2: 'empenhado',
    #     3: 'liquidado',
    # }
    # _states_nums = {v: k for k, v in _states_names.items()}
    # state = Column(db.Integer, default=0)
    state = Column(db.Enum('orcado', 'atualizado', 'empenhado', 'liquidado',
                           name='expense_state'),
                   default=0)

    # def get_state(self):
    #     return Execucao._states_names[self.state]

    # def set_state(self, state_name):
    #     self.state = Execucao._states_nums[state_name]

    # @classmethod
    # def get_num_state(cls, state_name):
    #     return Execucao._states_nums[state_name]

    @classmethod
    def get_year(cls):
        return cls.data['cd_anoexecucao'].cast(db.Integer)

    @classmethod
    def point_found(cls):
        return cls.point != None

    @staticmethod
    def get_region(point):
        """Returns the region of a point."""
        return Regions.query.filter(Regions.geo.ST_Contains(point))


class Regions(db.Model):

    __tablename__ = 'regions'

    id = Column(db.Integer, primary_key=True)
    name = Column(db.String)
    geo = Column(Geometry('POLYGON'))

    @staticmethod
    def get_points(region):
        """Returns the points inside a region."""
        return Execucao.query.filter(Execucao.point.ST_Within(region))
