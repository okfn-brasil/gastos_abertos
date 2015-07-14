#!/usr/bin/env python
# coding: utf-8

from __future__ import unicode_literals  # unicode by default
import unicodedata


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def canonical_form(s):
    # TODO: usar .casefold() instead of .lower() in Python 3
    return strip_accents(s).lower()
