# -*- coding: utf-8 -*-
"""
    Utils has nothing to do with models and views.
"""

import os


def make_dir(dir_path):
    try:
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
    except Exception, e:
        raise e



