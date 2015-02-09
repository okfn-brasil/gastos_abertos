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

    __tablename__ = 'revenue_code'

    id = Column(db.Integer, primary_key=True)
    code = Column(db.String(30), nullable=False)
    # code = Column(db.String(30), primary_key=True)
    description = Column(db.Text(), nullable=False)

    @staticmethod
    def format_code(code):
        # return '.'.join([str(int(i)) for i in code.split('.')])
        a, b, c = [str(int(i)) for i in code.split('.')]
        a = '.'.join(a)
        if int(c):
            formated = '.'.join([a, b, c])
        elif int(b):
            formated = '.'.join([a, b])
        else:
            formated = a.replace('0', '').strip('.')
        return formated
