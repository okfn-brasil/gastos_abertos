#!/usr/bin/env python
# coding: utf-8


import sys

from gastosabertos import create_app


def get_db():
    from gastosabertos.extensions import db
    app = create_app()
    db.app = app
    return db


class ProgressCounter(object):

    """Counts the progress percentage of a process."""

    def __init__(self, total, print_abs=False):
        """Init the counter with the total amout of steps needed to complete the
        process."""
        self.total = total
        self.current = 0.0
        self.print_abs = print_abs

    def update(self, increment=1):
        """Increment the number of steps done and prints the percentage."""
        self.current += increment
        s = "\r%s%%" % int(self.current/self.total*100)
        if self.print_abs:
            s += " (%s/%s)" % (int(self.current), self.total)
        sys.stdout.write(s)
        sys.stdout.flush()

    def end(self):
        """Prints the ender 100% (followed by a new line)."""
        sys.stdout.write("\r100%\n")
        sys.stdout.flush()
