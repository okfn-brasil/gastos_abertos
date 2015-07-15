# -*- coding: utf-8 -*-

from sqlalchemy import Column
from sqlalchemy.dialects import postgresql
from geoalchemy2 import Geometry

from ..extensions import db


class Execucao(db.Model):

    __tablename__ = 'execucao'

    id = Column(db.Integer, primary_key=True)
    data = Column(postgresql.JSONB)
    point = Column(Geometry('POINT'), default=None)
    # If an attempt were made to gecode this row
    searched = Column(db.Boolean, default=False)
    # code = Column(db.String(50), nullable=False)

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
