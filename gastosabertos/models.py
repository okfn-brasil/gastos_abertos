# -*- coding: utf-8 -*-

from sqlalchemy import Column, types

from .extensions import db

Model = db.Model

__all__ = ['db', 'Column', 'types', 'Model']
