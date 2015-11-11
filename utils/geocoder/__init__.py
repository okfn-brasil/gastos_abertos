#!/usr/bin/v python
# coding: utf-8

from __future__ import unicode_literals  # unicode by default
import os
import shelve
import json

import geopy
from shapely.geometry import shape, Point

from .utils import canonical_form
from .terms import TermsDB


# São Paulo bondaries
# ('-24.0069999', '-23.3569999', '-46.8264086', '-46.3650897')

class GeoEntity(object):
    """Stores information about an entity possibly geocoded."""

    def __init__(self, terms=None):
        self.terms = terms
        terms.sort(reverse=True, key=lambda x: x['weight'])
        self.region = ''

    def geocode(self, geocoder):
        """Geocodes all the terms of this entity"""
        for term in self.terms:
            # No need to geocode regions
            if not term.get('region'):
                geo = geocoder.geocode(term['string'])
                if geo:
                    term['geo'] = geo
                    if not self.region:
                        # TODO: descobrir regiao do ponto
                        self.region = "???"
            else:
                self.region = term['region']

    def best_coords(self):
        """Returns the best latitude, longitude and region found for this
        entity."""
        lat, lon = None, None
        for term in self.terms:
            # print(term)
            # print(term['weight'])
            geo = term.get("geo")
            if geo:
                osm = geo['osm']
                gm = geo['gm']
                geo_data = None
                if osm:
                    geo_data = osm
                elif gm:
                    geo_data = gm
                if geo_data:
                    g = geo_data[0]
                    lat, lon = g['latitude'], g['longitude']
                    break
        return lat, lon, self.region


class Geocoder(object):
    """A class to organize geoservers and geocode terms"""

    def __init__(self, data_folder="data", terms_folder="terms"):
        self.cache = shelve.open(os.path.join(data_folder, "cache.db"))
        self.terms_db = TermsDB(terms_folder)

        # Coords limits for geolocation
        #         bot  left    top     right
        self.limits = (-47, -24.05, -46.30, -23.35)
        self.regions = None

        self.osm = geopy.Nominatim(view_box=self.limits)
        self.gm = geopy.GoogleV3()
        self.server_options = {
            "osm": self.geocode_osm,
            "gm": self.geocode_gm,
        }
        self.shapefy_regions(os.path.join(data_folder,
                                          "subprefeituras.geojson"))

    def shapefy_regions(self, path_geojson):
        # TODO: permitir configurar...
        with open(path_geojson, 'r') as f:
            self.regions = {}
            j = json.load(f)
            for region in j['features']:
                name = region['properties']['name']
                poly = shape(region['geometry'])
                self.regions[name] = poly

    def inside_limits(self, point):
        """Checks if point is inside coords limits or possible region."""
        if not self.regions:
            # Use rectangle check
            lat, lon = point.latitude, point.longitude
            if (lon > self.limits[0] and lat > self.limits[1] and
               lon < self.limits[2] and lat < self.limits[3]):
                return True
            else:
                return False
        else:
            # Check inside all possible regions
            p = Point((point.longitude, point.latitude))
            for name, poly in self.regions.items():
                # if poly.contains(p):
                if p.intersects(poly):
                    return name
            return False

    def geocode(self, term):
        """Geocodes a term in all avaiable geoservers"""
        # TODO: permitir cofigurar isso...
        # limit string size
        s = term[:60]
        # check cache
        # TODO: remove this .encode for Python 3
        print(s)
        cache_key = s.encode("utf-8")
        term_geo = self.cache.get(cache_key)
        if not term_geo:
            term_geo = {}
            # query all servers
            for server_name, func in self.server_options.items():
                try:
                    points = func(s)
                except geopy.exc.GeocoderQuotaExceeded:
                    print("Quota Exceeded!")
                    raise
                except (geopy.exc.GeocoderTimedOut,
                        geopy.exc.GeocoderUnavailable):
                    print("Timed out or unable to contact server!")
                    print("Trying again...")
                    points = func(s)

                term_geo[server_name] = []
                for point in points:
                    region = self.inside_limits(point)
                    if region:
                        if region is True:
                            region = "???"
                        term_geo[server_name].append({
                            "address": point.address,
                            "latitude": point.latitude,
                            "longitude": point.longitude,
                            "region": region
                        })
            self.cache[cache_key] = term_geo
        # print("------------------------------------")
        # print(term_geo)
        return term_geo

    def geocode_osm(self, s):
        # TODO: permitir configurar
        s += ", São Paulo, São Paulo"
        r = self.osm.geocode(s, timeout=10, exactly_one=True)
        if r:
            return [r]
        else:
            return []

    def geocode_gm(self, s):
        # TODO: permitir configurar
        s += ", São Paulo, São Paulo"
        r = self.gm.geocode(s, timeout=10, exactly_one=True)
        # TODO: permitir configurar
        if not r or r.address == "São Paulo - State of São Paulo, Brazil":
            return []
        else:
            return [r]

    def geocode_list(self, strings):
        """Creates a GeoEntity with the strings geocoded."""
        all_terms = []
        for string in strings:
            canonical = canonical_form(string)
            terms = self.terms_db.search(string, canonical)
            if terms:
                all_terms += terms
        gt = GeoEntity(all_terms)
        gt.geocode(self)
        return gt
        # return self.geocode(all_terms)

    def close(self):
        """Closes cache."""
        self.cache.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
