# -*- coding: utf-8 -*-

from sqlalchemy import Column
from sqlalchemy.dialects import postgresql
from geoalchemy2 import Geometry

from ..extensions import db


class Execucao(db.Model):

    __tablename__ = 'execucao'

    id = Column(db.Integer, primary_key=True)
    # code = Column(db.String(50), nullable=False)
    data = Column(postgresql.JSONB)
    point = Column(Geometry('POINT'))
