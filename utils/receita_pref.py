#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import glob
import pandas as pd


if 'unix' not in csv.list_dialects():
    csv.register_dialect('unix', lineterminator='\n')


class CodeTable(object):
    """Indexes descriptions for codes"""
    def __init__(self, filename):
        self.filename = filename
        self.reload()

    def reload(self):
        try:
            self.df = pd.read_csv(self.filename, index_col='codigo')
        except:
            self.df = pd.DataFrame({'codigo': [], 'descricao': []})
            self.df.set_index('codigo')

    def save(self):
        self.df.to_csv(self.filename)

    def __getitem__(self, cod):
        return self.df.at[ cod,'descricao']

    def __setitem__(self, cod, descr):
        current = ''
        try:
            current = self.df.at[cod,'descricao']
        except:
            pass
        if len(descr) > len(current):
            self.df.at[cod,'descricao'] = descr
        

def rename_dirty_fieldnames(dirty_csv):
    # Renaming dirty named columns
    dirty_csv.fieldnames[1] = "previsto_categoria"
    dirty_csv.fieldnames[2] = "realizado_categoria"
    dirty_csv.fieldnames[4] = "previsto_fonte"
    dirty_csv.fieldnames[5] = "realizado_fonte"
    dirty_csv.fieldnames[7] = "previsto_especificacao"
    dirty_csv.fieldnames[8] = "realizado_especificacao"
    dirty_csv.fieldnames[10] = "previsto_rubrica"
    dirty_csv.fieldnames[11] = "realizado_rubrica"
    dirty_csv.fieldnames[13] = "previsto_alinea"
    dirty_csv.fieldnames[14] = "realizado_alinea"
    dirty_csv.fieldnames[16] = "previsto_subalinea"
    dirty_csv.fieldnames[17] = "realizado_subalinea"
    dirty_csv.fieldnames[18] = "previsto_mensal"
    dirty_csv.fieldnames[19] = "realizado_mensal"
    dirty_csv.fieldnames[20] = "previsto_total"
    dirty_csv.fieldnames[21] = "realizado_total"

def get_cod_descr(row):
    """Search for the most 'deep' description in the row and returns its code
    and description"""
    hierarchy = ['Descrição_Categoria_Código', 'Descrição_Fonte_Código',
                 'Descrição_Especificação_Código', 'Descrição_Rubrica_Código',
                 'Descrição_Alínea_Código', 'Descrição_Sub_Alínea_Código']
    cod = ''
    try:
        while not cod.strip():
            cod = row[hierarchy.pop()]
    except IndexError:
        print("Error: no 'codigo' found in %s" % row)
    cod, _, descr = cod.partition('-')
    if not cod:
        print("Error: 'codigo' seems invalid in %s" % row)
    return cod.strip(), descr.strip()

def read_row_csvfile(filename, codetable=None):
    """Generator that opens a CSV, and returns rows doing some cleaning. If a
    codetable is passed, updates it."""
    with open(filename, 'r') as file:
        year = os.path.basename(filename).rpartition('.')[0]
        csv_lines = file.readlines()[7:]
        dirty_csv = csv.DictReader(csv_lines, dialect='unix')
        rename_dirty_fieldnames(dirty_csv)

        # Returning rows with date info
        prev_cod = None
        for row in dirty_csv:
            cod, descr = get_cod_descr(row)
            if codetable:
                codetable[cod] = descr
            row['codigo'] = cod
            # Reset month count when entering new codigo group
            if cod != prev_cod or month > 12:
                month = 1
            row['data'] = "%s-%s" % (year, str(month).zfill(2))
            #for key, value in row.items():
            #    if key.split('_')[0] == "Descrição":
            #        row[key] = value.partition('-')[2].strip()
            month += 1
            prev_cod = cod
            yield row

def join_csvs(folder, outfilename, fieldnames, codetable=None):
    csvfiles = glob.glob(os.path.join(folder, "[0-9][0-9][0-9][0-9].csv"))
    csvfiles.sort()

    with open(outfilename, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, dialect='unix')
        writer.writeheader()
        for csvfile in csvfiles:
            num_rows = 0
            for row in read_row_csvfile(csvfile, codetable):
                writer.writerow(row)
                num_rows += 1
            if num_rows % 12 != 0:
                print("""Warning: The number of rows in %s is not multiple of
                      12, and it probably should be (one row for month), unless
                      you are processing an unfinished year.""")

def compress_csv(filename, outfilename, codetable):
    df = pd.read_csv(filename)
    df['descricao'] = pd.Series([''] * len(df))
    df = df[['data','codigo','descricao','previsto_mensal','realizado_mensal']]
    for i in range(len(df)):
        cod = df.iat[i, 1]
        df.at[i, 'descricao'] = codetable[cod]
    df.to_csv(outfilename)


if __name__ == '__main__':

    fieldnames = ['data', 'codigo', 'Descrição_Categoria_Código',
                  'previsto_categoria', 'realizado_categoria',
                  'Descrição_Fonte_Código', 'previsto_fonte', 'realizado_fonte',
                  'Descrição_Especificação_Código', 'previsto_especificacao',
                  'realizado_especificacao', 'Descrição_Rubrica_Código',
                  'previsto_rubrica', 'realizado_rubrica',
                  'Descrição_Alínea_Código', 'previsto_alinea',
                  'realizado_alinea', 'Descrição_Sub_Alínea_Código',
                  'previsto_subalinea', 'realizado_subalinea',
                  'previsto_mensal', 'realizado_mensal',
                  'previsto_total', 'realizado_total']

    folder = '../../gastos_abertos_dados/Orcamento/receitas/via_site_prefeitura/'
    outfilename = "receitas.csv"
    outcompressed = "receitas_min.csv"
    codetable = CodeTable("cods_descr.csv")
    join_csvs(folder, outfilename, fieldnames, codetable)
    codetable.save()
    compress_csv(outfilename, outcompressed, codetable)
