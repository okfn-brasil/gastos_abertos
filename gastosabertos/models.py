# -*- coding: utf-8 -*-

import inspect
import collections
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
    ES_String: {'index_analyzer': 'ga_analyzer',
                'search_analyzer': 'ga_search_analyzer'}
}

# FIXME: should be configurable
INDEX = 'gastos_abertos'
HOSTS = ['localhost']
es_connection = es_connections.create_connection(hosts=HOSTS)


def _create_es_field(sa_type, es_kwargs, boost=None):
    if not inspect.isclass(sa_type):
        sa_type = sa_type.__class__

    es_type = SA_TO_ES.get(sa_type, None)
    if not es_type:
        return None

    es_kwargs = es_kwargs or {}
    es_kwargs_ = {}
    if not 'index' in es_kwargs or es_kwargs['index'] is not 'not_analyzed':
        es_kwargs_ = ES_DEFAULT_OPTIONS.get(es_type, {})
    es_kwargs_.update(es_kwargs)

    field = es_type(**es_kwargs_)
    field._search_boost = boost

    return field


def _get_sa_columns(model):
    mapper = class_mapper(model)
    return mapper.columns


def _get_fieldname(field):
    if isinstance(field, basestring):
        fieldname = field
    elif isinstance(field, property):
        fieldname = field.fget.__name__
    else:
        fieldname = field.key
    return fieldname


def _create_es_doctype_class(name, index, fields):
    attrs = {}
    attrs.update(fields)
    attrs['Meta'] = type('Meta', (), {'index': index})
    cls = type(name, (ES_DocType, ), attrs)
    #cls.init()
    return cls


def _remove_boost(fieldnames):
    import re
    without_boost = []
    for fieldname in fieldnames:
        without_boost.append(re.sub(r'\^.*', '', fieldname))
    return without_boost


class Column(SA_Column):
    def __init__(self, type_, primary_key=False,
                 searchable=None, search_config=None, boost=None,
                 *args, **kwargs):
        super(Column, self).__init__(type_, primary_key=primary_key,
                                     *args, **kwargs)

        if searchable is None and search_config:
            searchable = True

        if searchable or primary_key:
            self._es_field = _create_es_field(type_, search_config, boost)

    def _constructor(self, name, type_, **kw):
        col = Column(type_, **kw)
        col.name = name
        return col


class ES_QuerySet(object):

    def __init__(self, model, search, vals=None, highlight=False):
        self._model = model
        self._search = search
        self._response = None
        self._sqlalchemy_query = None
        self._filter_params = {}
        self._sa_order_by = []
        self._highlight = highlight
        self._vals = {
            'pos': 0,
            'size': 100,
        }
        self._vals.update(vals or {})

    def _execute(self):
        print self._search.to_dict()
        if not self._response:
            self._response = self._search.execute()
        return self._response

    def _do_sqlalchemy_query(self):
        if not self._sqlalchemy_query:
            self._sqlalchemy_query = self._model.filter(
                self._model.id.in_([r._id for r in self._response]))
        if self._sa_order_by:
            self._sqlalchemy_query = self._sqlalchemy_query.order_by(*self._sa_order_by)
        return self._sqlalchemy_query

    def from_self(self):
        return self

    def all(self):
        self._execute()
        self._do_sqlalchemy_query()
        result = self._sqlalchemy_query.all()
        meta = {int(response.meta.id): response.meta for response in self._response}
	return_ = []
        for element in result:
            if self._highlight:
                for fieldname, highlighted in meta[element.id].highlight.to_dict().items():
                    try:
                        for partial in highlighted:
                            partial_orig = partial.replace('<b>', '').replace('</b>', '')
                            setattr(element, fieldname, getattr(element, fieldname).replace(partial_orig, partial))
                    except AttributeError:
                        try:
                            excerpt = u' ... '.join(highlighted).replace('\n', ' ')
                            setattr(element, '{}_highlight'.format(fieldname), u'... {} ...'.format(excerpt))
                        except (UnicodeEncodeError, AttributeError):
                            pass
            return_.append(element)
	return return_

    def count(self):
        self._execute()
        return self._response.hits.total

    def order_by(self, *args, **kwargs):
        _order_by = []
        self._sa_order_by = args[:] 
        for arg in args:
            try:
                colname = arg.get_children()  # see sqlalchemy UnaryExpression
                modifier = '-'
                colname = str(colname[0])
            except AttributeError:
                modifier = ''
                colname = arg
            _order_by.append('{}{}'.format(modifier, colname))
        self._search = self._search.sort(*_order_by)
        return self

    def filter(self, *args, **kwargs):
        for expr in args:
            left, right = expr.get_children()  # see sqlalchemy BinaryExpression
            operator = expr.operator.__name__
            try:
                colname = left.name  # see sqlalchemy Column
            except AttributeError:
                colname = left
            try:
                value = right.effective_value  # see sqlalchemy BindParameter
            except AttributeError:
                value = right
            if operator is 'ilike_op':
                value = value.replace('%', '').lower()
                search = self._search.query(ES_Q('match_phrase', **{colname: value}))
            else:
                search = self._search.query(ES_Q('match', **{colname: value}))
            self._filter_params[colname] = value
        return self.__class__(self._model, search, self._vals, self._highlight)

    def offset(self, pos):
        vals = self._vals.copy()
        vals.update(pos=pos)
        start = pos
        stop = start + vals['size']
        return self.__class__(self._model, self._search[start:stop], vals, self._highlight)

    def limit(self, size):
        vals = self._vals.copy()
        vals.update(size=size)
        start = vals['pos']
        stop = start + size
        return self.__class__(self._model, self._search[start:stop], vals, self._highlight)


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
            cls._es_string_fieldnames_with_boost = [
                ('{}^{}'.format(f, t._search_boost)
                 if t._search_boost else f) for f, t in fields.iteritems()
                if isinstance(t, ES_String)]
            # save reference to elasticsearch DocType class
            cls._es_doctype = _create_es_doctype_class(
                name, INDEX, fields)


class _SearchableMixin(object):
    @classmethod
    def _prepare_search(cls, query, fields=None):
        if fields and not (isinstance(fields, collections.Iterable) and
                not isinstance(fields, basestring)):
            fields = [fields]
        fieldnames = ([_get_fieldname(f) for f in fields] if fields else
                     cls._es_string_fieldnames_with_boost)
        s = ES_Search().doc_type(cls._es_doctype)
        # Get only ids to avoid json decode issues
        s.update_from_dict({'fields': 'id'})
        return s, fieldnames

    @classmethod
    def fuzzy(cls, query, fields=None, highlight=False):
        s, fieldnames = cls._prepare_search(query, fields=None)
        highlight_ = False
        if query:
            s = s.query(ES_Q('fuzzy_like_this', like_text=query,
                             analyzer='ga_search_analyzer',
                             fields=fieldnames))
            if highlight:
                s = s.highlight(*_remove_boost(fieldnames), 
                               pre_tags=['<b>'], post_tags=['</b>'], 
                               fragment_size=400, number_of_fragments=3)
                highlight_ = True
        return ES_QuerySet(model=cls, search=s, highlight=highlight_)

    @classmethod
    def search(cls, query, fields=None, default_operator='and', fuzzy=False, highlight=False):
        if query and fuzzy:
            return cls.fuzzy(query, fields, highlight)
        s, fieldnames = cls._prepare_search(query, fields=None)
        highlight_ = False
        if query:
            s = s.query(ES_Q('simple_query_string', query=query,
                             analyzer='ga_search_analyzer',
                             default_operator=default_operator,
                             fields=fieldnames))
            if highlight:
                s = s.highlight(*_remove_boost(fieldnames), 
                               pre_tags=['<b>'], post_tags=['</b>'], 
                               fragment_size=400, number_of_fragments=3)
                highlight_ = True
        return ES_QuerySet(model=cls, search=s, highlight=highlight_)

    @classmethod
    def suggestions(cls, text, field=None):
        fieldname = _get_fieldname(field)

        response = es_connection.suggest({
            'suggestion': {
                'text': text,
                'term': {
                    'field': fieldname,
                    'analyzer': 'standard',
                }
            }
        }, index=INDEX)
        return [s['options'] for s in response['suggestion']]

    @classmethod
    def suggestion(cls, text, field=None):
        if not field:
            field = cls._es_fields[0]
        suggestions = cls.suggestions(text, field)
        words = text.split()
        new = []
        for i, suggestion in enumerate(suggestions):
            if suggestion:
                new.append(suggestion[0]['text'])
            else:
                new.append(words[i])
        return u' '.join(new)

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
