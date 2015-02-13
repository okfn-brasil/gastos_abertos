# -*- coding: utf-8 -*-

''' Validates a Json file using a Json Schema.

Usage:
    ./validate [SCHEMA_FILE] [JSON_FILE]
    ./validate (-h | --help)

Options:
    -h --help   Show this message.
'''

import json
from jsonschema import validate
from docopt import docopt


def validate_file(schema_file, json_file):
    with open(schema_file, 'r') as s:
        with open(json_file, 'r') as j:
            schema = json.load(s)
            json_ = json.load(j)
            if not validate(json_, schema):
                print("Seems legit.")


if __name__ == '__main__':
    arguments = docopt(__doc__)
    schema_file = arguments['SCHEMA_FILE']
    json_file = arguments['JSON_FILE']
    if schema_file:
        if not json_file:
            json_file = schema_file.replace('.', '-test.')
        validate_file(schema_file, json_file)
    else:
        print("Need args!")
