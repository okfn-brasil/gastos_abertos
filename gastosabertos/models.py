# -*- coding: utf-8 -*-

import inspect
from functools import wraps
from concurrent import futures

from sqlalchemy import Column as SA_Column
from sqlalchemy.ext.declarative import AbstractConcreteBase
from sqlalchemy.orm import class_mapper

from elasticsearch_dsl import (
    DocType as ES_DocType,
    Boolean as ES_Boolean,
    # Byte as ES_Byte,
    Date as ES_Date,
    # Double as ES_Double,
    Float as ES_Float,
    Integer as ES_Integer,
    String as ES_String,
)
from elasticsearch_dsl.connections import connections as es_connections

from flask.ext.sqlalchemy import _BoundDeclarativeMeta

from .extensions import db


__all__ = ['Model', 'Column', 'searchable']

SA_TO_ES = {
    db.Boolean: ES_Boolean,
    db.Date: ES_Date,
    # db.DateTime: ???,
    db.Float: ES_Float,
    db.Integer: ES_Integer,
    db.String: ES_String,
    db.Text: ES_String,
    # db.Time: ???,
    db.Unicode: ES_String,  # check
    db.UnicodeText: ES_String,  # check
}

# FIXME: should be configurable
es_connections.create_connection(hosts=['localhost'])


def _create_es_field(sa_type, es_kwargs):
    if not inspect.isclass(sa_type):
        sa_type = sa_type.__class__

    es_type = SA_TO_ES.get(sa_type, None)
    es_kwargs = es_kwargs or {}

    if not es_type:
        return None

    field = es_type(**es_kwargs)

    return field


def _get_sa_columns(model):
    mapper = class_mapper(model)
    return mapper.columns


def _create_es_doctype_class(name, index, fields):
    attrs = {}
    attrs.update(fields)
    attrs['Meta'] = type('Meta', (), {'index': index})
    cls = type(name, (ES_DocType, ), attrs)
    cls.init()
    return cls


class Column(SA_Column):
    def __init__(self, type_, primary_key=False,
                 searchable=None, search_config=None,
                 *args, **kwargs):
        super(Column, self).__init__(type_, primary_key=primary_key,
                                     *args, **kwargs)

        if searchable is None and search_config:
            searchable = True

        if searchable or primary_key:
            self._es_field = _create_es_field(type_, search_config)

    def _constructor(self, name, type_, **kw):
        col = Column(type_, **kw)
        col.name = name
        return col


class _SearchableMixinMeta(type):
    def __init__(cls, name, bases, attrs):
        super(_SearchableMixinMeta, cls).__init__(name, bases, attrs)
        name = cls.__name__
        if name is not 'Model':
            tablename = attrs.get('__tablename__')
            fields = {}
            for k, v in attrs.iteritems():
                if hasattr(v, '_es_field'):
                    # get elastcsearch fields from sqlalchemy columns
                    fields[k] = v._es_field
                elif hasattr(v, 'fget') and hasattr(v.fget, '_es_field'):
                    # get elastcsearch fields from properties
                    fields[k] = v.fget._es_field
            # save reference to elasticsearch field names
            cls._es_fieldnames = [name for name, _ in fields.iteritems()]
            # save reference to elasticsearch DocType class
            cls._es_doctype = _create_es_doctype_class(name, tablename, fields)


class _SearchableMixin(object):
    @classmethod
    def search(cls, query):
        pass

    @classmethod
    def build_search_index(cls, reset=True):
        # TODO: use the reset arg

        def add_to_index(id_, db, app):
            with app.app_context():
                obj = db.session.query(cls).get(id_)
                obj.add_to_search_index()

        app = db.get_app()
        with futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_id = dict((executor.submit(add_to_index, id_, db, app), id_)
                                 for id_ in xrange(cls.count() + 1))

            for future in futures.as_completed(future_to_id):
                id = future_to_id[future]
                if future.exception() is not None:
                    print('%r generated an exception: %s' % (id,
                                                             future.exception()))

    def add_to_search_index(self):
        kwargs = self._get_searchable_values()
        doc = self._es_doctype(**kwargs)
        doc.meta.id = self.id
        doc.save()

    def _get_searchable_values(self):
        values = {}
        for fieldname in self._es_fieldnames:
            values[fieldname] = getattr(self, fieldname)
        return values


class ModelMeta(_BoundDeclarativeMeta, _SearchableMixinMeta):
    def __init__(cls, name, bases, attrs):
        _BoundDeclarativeMeta.__init__(cls, name, bases, attrs)
        _SearchableMixinMeta.__init__(cls, name, bases, attrs)


class Model(AbstractConcreteBase, db.Model, _SearchableMixin):

    __metaclass__ = ModelMeta

    @classmethod
    def query(cls, opts=None):
        if not opts:
            opts = cls
        return db.session.query(opts)

    @classmethod
    def filter(cls, filter=None):
        query = cls.query(cls)
        if filter:
            query = query.filter(filter)
        return query

    @classmethod
    def count(cls):
        return cls.query().count()


def searchable(type_=None, search_config=None):
    search_config = search_config or {}

    def searchable_decorator(func):
        @wraps(func)
        def decorated_func(*args, **kwargs):
            return func(*args, **kwargs)
        decorated_func._es_field = _create_es_field(type_, search_config)
        return decorated_func

    return searchable_decorator
