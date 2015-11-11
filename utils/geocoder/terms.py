#!/usr/bin/env python
# coding: utf-8

from __future__ import unicode_literals  # unicode by default
import os
import re
import codecs

from .utils import canonical_form


# Backports fullmatch for Python < 3.4
try:
    re.fullmatch
except:
    # https://stackoverflow.com/questions/30212413/backport-python-3-4s-regular-expression-fullmatch-to-python-2
    def fullmatch(regex, string, flags=0):
        """Emulate python-3.4 re.fullmatch()."""
        return re.match("(?:" + regex + r")\Z", string, flags=flags)
    re.fullmatch = fullmatch


class Term(object):
    """Class for a term that preceeds the name of a place.
    The real name of the place must be after it.
    E.g.: Rua ..."""

    # pattern used to call this class in a term file
    define_pattern = r">> {class_name} (?P<weight>\d+)"

    # pattern used to search for this term in text
    pattern = r"(?:\W|^)({term}[ s][^-,]+)(.*)"

    def __init__(self, terms, excep, canonize, weight=0):
        if weight is None:
            weight = 0
        self.weight = int(weight)
        self.canonize = canonize
        if self.canonize:
            terms = canonical_form(terms)

        if excep:
            self.pattern = r"(?<!{excep})\s*" + self.pattern
            if self.canonize:
                excep = canonical_form(excep)

        self.pattern = re.compile(self.pattern.format(term=terms, excep=excep))

    def compare(self, noncanonical, canonical):
        if self.canonize:
            text = canonical
        else:
            text = noncanonical
        all_found = re.search(self.pattern, text)
        if all_found:
            return {
                "string": all_found.group(1),
                "weight": self.weight
            }
        else:
            return None


class Name(Term):
    """Class for a name of a place.
    The name by itself should be enough to locate a place.
    E.g.: Câmara Municipal"""

    # pattern used to search for this term in text
    pattern = r"(?:\W|^)({term})(\W|$)"


class Region(Name):
    """Class for a name of a region.
    The name by itself should be enough to locate a region.
    E.g.: Parelheiros"""

    # pattern used to call this class in a term file
    define_pattern = (
        r">> {class_name} (?P<region>[^|]+)(?: | (?P<weight>\d+))?"
    )

    def __init__(self, terms, excep, canonize, region, weight=0):
        """Calls super's init and adds 'region' attr"""
        super(Region, self).__init__(terms, excep, canonize, weight)
        self.region = region

    def compare(self, noncanonical, canonical):
        """Calls super's compare and adds 'region' attr"""
        found = super(Region, self).compare(noncanonical, canonical)
        if found:
            found['region'] = self.region
        return found


def get_all_subclasses(cls):
    """Return the cls and its subclasses"""
    all_subclasses = [cls]
    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))
    return all_subclasses


def check_class(line):
    """Check if line defines a class"""
    for c in get_all_subclasses(Term):
        patt = c.define_pattern.format(class_name=c.__name__)
        matched = re.fullmatch(patt, line)
        if matched:
            return c, matched.groupdict()
    return None


class TermsDB(object):
    """Class to load and search the REs"""

    def __init__(self, folder="terms"):
        self.tokens = []
        self.load_folder(folder)

    def load_folder(self, path):
        """Load tokens from a folder"""
        files = os.listdir(path)
        for file_name in files:
            if file_name[0] != '.':
                with codecs.open(os.path.join(path, file_name),
                                 'r', 'utf-8') as f:
                    self.load_text(f.read())

    def load_text(self, text):
        """Load tokens from a text"""
        for line in text.splitlines():
            # ignore lines started with '#'
            if line and line[0] != "#":
                new_class = check_class(line)
                if new_class:
                    current_class, current_args = new_class
                # do not canonize if line starts with '!'
                # alias are separeted with '|'
                # use '?!' for except if preceded by
                patt = r"(?P<canonize>!)?(?P<terms>.+?)((\?\!)(?P<excep>.*))?"
                dic = re.fullmatch(patt, line).groupdict()
                canonize = not dic["canonize"]
                excep = dic["excep"]
                terms = dic["terms"]
                token = current_class(terms, excep, canonize, **current_args)
                self.tokens.append(token)

    def search(self, noncanonical, canonical):
        """Search all the terms for matching ones and return them."""
        all_found = []
        for token in self.tokens:
            found = token.compare(noncanonical, canonical)
            if found:
                all_found.append(found)
        return all_found


# TERMSDB = TermsDB()


# EXPS = {
#     "rua": ("em situação de", ("rua", "r\.")),
#     "avenida": ("", ("avenida", "av\.")),
#     "jardim": ("", ("jardim", "jd\.")),
#     "travessa": ("", ("travessa", "trav.")),
#     "favela": "",
#     "praca": "",
#     "viela": "",
#     # "operacao urbana": "",
#     "vila": "",
#     "ponte": "",
#     "parque": "",
#     "escola": "",
#     "bairro": "",
#     "quadra": "",
#     "corrego": "",
#     "viaduto": "",
#     "ladeira": "",
#     "ginasio": "",
#     "chacara": "",
#     "estadio": "",
#     "hospital": "",
#     "distrito": "",
#     "autodromo": "",
#     "cemiterio": "",
#     "instituto": "",
#     "biblioteca": "",
#     "maternidade": "",
#     "Polo Cultural": "",
#     "Centro Cultural": "",
#     "Complexo Esportivo": "",
#     "Conjunto Habitacional": "",
#     "Conservatório Musical": "",
#     "UBS": "",
#     "CDC": "",
#     "emef": "",
#     "emei": "",
#     "cei": "",
#     "ceu": "",
#     "apa": "",
# }
# # EXPS["subprefeitura"] = ("", subs)
