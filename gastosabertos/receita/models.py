# -*- coding: utf-8 -*-

from sqlalchemy import Column, types

from ..extensions import db


class Revenue(db.Model):

    __tablename__ = 'revenue'

    id = Column(db.Integer, primary_key=True)
    original_code = Column(db.String(30), nullable=False)
    code = db.relationship('RevenueCode', backref='revenues')
    code_id = Column(db.Integer, db.ForeignKey('revenue_code.id'), nullable=True)
    description = Column(db.Text(), nullable=False)
    date = Column(db.DateTime())
    monthly_predicted = Column(db.DECIMAL(19, 2))
    monthly_outcome = Column(db.DECIMAL(19, 2))
    economical_category = Column(db.Integer)
    economical_subcategory = Column(db.Integer)
    source = Column(db.Integer)
    rubric = Column(db.Integer)
    paragraph = Column(db.Integer)
    subparagraph = Column(db.Integer)


class RevenueCode(db.Model):

    __tablename__ = 'revenue_code'

    id = Column(db.Integer, primary_key=True)
    code = Column(db.String(30), nullable=False)
    # code = Column(db.String(30), primary_key=True)
    description = Column(db.Text(), nullable=False)

    @staticmethod
    def format_code(code):
        ns = [str(int(i)) for i in code.split('.')]
        ns[0] = '.'.join(ns[0])
        if len(ns) > 2 and int(ns[2]):
            formated = '.'.join([ns[0], ns[1], ns[2]])
        elif len(ns) > 1 and int(ns[1]):
            formated = '.'.join([ns[0], ns[1]])
        else:
            formated = ns[0].replace('0', '').strip('.')
        return formated
