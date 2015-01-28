# -*- coding: utf-8 -*-

from sqlalchemy import Column, types

from ..extensions import db


class Revenue(db.Model):

    __tablename__ = 'revenue'

    id = Column(db.Integer, primary_key=True)
    code = Column(db.String(), nullable=False)
    description = Column(db.String(), nullable=False)
    date = Column(db.Date())
    monthly_predicted = Column(db.DECIMAL(19,2))
    monthly_outcome = Column(db.DECIMAL(19,2))
