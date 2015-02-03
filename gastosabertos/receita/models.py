# -*- coding: utf-8 -*-

from sqlalchemy import Column, types

from ..extensions import db


class Revenue(db.Model):

    __tablename__ = 'revenue'

    id = Column(db.Integer, primary_key=True)
    code = Column(db.String(30), nullable=False)
    description = Column(db.Text(), nullable=False)
    date = Column(db.Date())
    monthly_predicted = Column(db.DECIMAL(19, 2))
    monthly_outcome = Column(db.DECIMAL(19, 2))
    economical_category = Column(db.Integer)
    economical_subcategory = Column(db.Integer)
    source = Column(db.Integer)
    rubric = Column(db.Integer)
    paragraph = Column(db.Integer)
    subparagraph = Column(db.Integer)


class RevenueCode(db.Model):

    __tablename__ = 'revenuecode'

    id = Column(db.Integer, primary_key=True)
    code = Column(db.String(30), nullable=False)
    description = Column(db.Text(), nullable=False)
