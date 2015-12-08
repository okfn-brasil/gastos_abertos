# -*- coding: utf-8 -*-

import os
from flask import Response, request


def make_dir(dir_path):
    try:
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
    except Exception, e:
        raise e


def default_json_to_csv(json_content):
    csv = ''
    wrote_header = False
    for line in json_content:
        if not wrote_header:
            csv +=  ','.join([u'"{}"'.format(v) for v in line.keys()]) + '\n'
            wrote_header = True

        for value in line.values()[:-1]:
            csv += u'"{}", '.format(value)
        csv += u'"{}"\n'.format(line.values()[-1])
    return csv


def with_csv(filename="output.csv", json_converter=default_json_to_csv):
    ''' Decorator to transforma an JSON output in a CSV in a naive way'''
    def with_csv_output(method):
        def view_plus_csv(*args, **kwargs):
            output_csv = bool(request.args.get('csv', False))

            if not output_csv:
                return method(*args, **kwargs)
            else:
                output = method(*args, **kwargs)

                csv = json_converter(output[0])
                return Response(csv,
                               mimetype="text/csv",
                               headers={"Content-Disposition": "attachment;filename={}".format(filename)})
        return view_plus_csv
    return with_csv_output


