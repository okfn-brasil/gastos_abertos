# -*- coding: utf-8 -*-

import inspect
from functools import wraps
from concurrent import futures
import unicodedata

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
    Index as ES_Index,
    Search as ES_Search,
    Q as ES_Q,
    analyzer as es_analyzer,
    tokenizer as es_tokenizer,
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

ES_DEFAULT_OPTIONS = {
    ES_String: {'index_analyzer': 'brazilian'}
}

# FIXME: should be configurable
INDEX = 'gastos_abertos'
HOSTS = ['localhost']
es_connections.create_connection(hosts=HOSTS)


def _create_es_field(sa_type, es_kwargs):
    if not inspect.isclass(sa_type):
        sa_type = sa_type.__class__

    es_type = SA_TO_ES.get(sa_type, None)
    if not es_type:
        return None

    es_kwargs_ = ES_DEFAULT_OPTIONS.get(es_type, {})
    es_kwargs_.update(es_kwargs or {})

    field = es_type(**es_kwargs_)

    return field


def _get_sa_columns(model):
    mapper = class_mapper(model)
    return mapper.columns


def _create_es_doctype_class(name, index, fields):
    attrs = {}
    attrs.update(fields)
    attrs['Meta'] = type('Meta', (), {'index': index})
    cls = type(name, (ES_DocType, ), attrs)
    #cls.init()
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
            cls._es_fieldnames = fields.keys()
            cls._es_string_fieldnames = [f for f, t in fields.iteritems()
                                         if isinstance(t, ES_String)]
            # save reference to elasticsearch DocType class
            cls._es_doctype = _create_es_doctype_class(
                name, INDEX, fields)


class _SearchableMixin(object):
    @classmethod
    def search(cls, query):
        s = ES_Search().doc_type(cls._es_doctype)
        s = s.query(ES_Q('simple_query_string', query=query,
                         analyzer='brazilian',
                         fields=cls._es_string_fieldnames))
        response = s.execute()
        return cls.filter(cls.id.in_([r.id for r in response]))

    @classmethod
    def build_search_index(cls, reset=True):
        if reset:
            index = ES_Index(cls._es_doctype._doc_type.index)
            index.delete(ignore=404)
            cls._es_doctype.init()


        def add_to_index(id_, db, app):
            with app.app_context():
                obj = db.session.query(cls).get(id_)
                obj.add_to_search_index()

        app = db.get_app()

        with futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_id = dict((executor.submit(add_to_index, id_, db, app),
                                 id_) for id_ in xrange(1, cls.count() + 1))

            for future in futures.as_completed(future_to_id):
                id = future_to_id[future]
                if future.exception() is not None:
                    print('%r generated an exception: %s' % (
                        id, future.exception()))

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
    def filter(cls, *args):
        query = cls.query(cls)
        if args:
            query = query.filter(*args)
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
