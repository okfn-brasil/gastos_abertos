# -*- coding: utf-8 -*-

def format_code(s):
    # return '.'.join([str(int(i)) for i in s.split('.')])
    a, b, c = [str(int(i)) for i in s.split('.')]
    a = '.'.join(a)
    if int(c):
        formated = '.'.join([a, b, c])
    elif int(b):
        formated = '.'.join([a, b])
    else:
        formated = a.replace('0', '').strip('.')
    return formated
