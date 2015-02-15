#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import os
import csv
from collections import OrderedDict

import bs4
#from xmlutils.xml2csv import xml2csv


class XMLCamara(object):

    def __init__(self, download_filename="dados_receita_camara.zip",
                 xmldir="balancetes_1988_2014",
                 csvdir="csvs_camara",
                ):
        self.download_filename = download_filename
        self.xmldir = xmldir
        self.csvdir = csvdir

    def download(self):
        commands = "wget http://www2.camara.sp.gov.br/CTEO/BalancetesXML/balancetes_1988_2014.zip -O".split()
        commands.append(self.download_filename)
        if subprocess.call(commands) != 0:
            print("Error to download")

    def unpack(self):
        commands = "unzip -o".split()
        commands.append(self.download_filename)
        if subprocess.call(commands) != 0:
            print("Error to unpack")

    def xml2dictlist(self, file):
        #converter = xml2csv(file, outfile, encoding=encoding)
        #converter.convert()
        with open(file, 'r') as xmlfile:
            xml = bs4.BeautifulSoup(xmlfile)
            #line_name = xml.contents[2].contents[1].name
            # Find out the name of tags used for the lines
            for root in xml.children:
                if isinstance(root, bs4.element.Tag):
                    for element in root.children:
                        # This should be the equivalent to a "line" in a CSV
                        if isinstance(element, bs4.element.Tag):
                            line_tag_name = element.name
                            break

            rows = []
            for row in xml.find_all(line_tag_name):
                dic = OrderedDict()
                for element in row.contents:
                    # These should be the columns of the table
                    if isinstance(element, bs4.element.Tag):
                        dic[element.name.encode('utf8')] = element.string.encode('utf8')
                rows.append(dic)
            return rows

    def dictlist2csv(self, file, dictlist):
        if 'unix' not in csv.list_dialects():
            csv.register_dialect('unix', lineterminator='\n')
        with open(file, 'wb') as csvfile:
            fieldsnames = dictlist[0].keys()
            writer = csv.DictWriter(csvfile, fieldsnames, dialect='unix')
            writer.writeheader()
            for row in dictlist:
                try:
                    writer.writerow(row)
                except:
                    print(row, dictlist.index(row))
                    raise

    def convert_all(self):
        xmls = os.listdir(self.xmldir)
        for xml in xmls:
            #xml = '1988.11.xml'
            csv = xml.rpartition('.')[0] + ".csv"
            dictlist = self.xml2dictlist(os.path.join(self.xmldir, xml))

            a = []
            for i in dictlist:
                try:
                    a.append(i['codigo'])
                except:
                    pass
            b = sorted(a)
            for i in range(len(b)):
                if b[i] != a[i]:
                    print(csv, b[i], "------------------------", a[i])

            self.dictlist2csv(os.path.join(self.csvdir, csv), dictlist)

    def process(self):
        self.download()
        self.unpack()
        if not os.path.exists(self.csvdir):
            os.mkdir(self.csvdir)
        self.convert_all()


x = XMLCamara()
x.process()
