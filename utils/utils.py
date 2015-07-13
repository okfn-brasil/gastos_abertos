#!/usr/bin/env python
# coding: utf-8


import sys


class ProgressCounter(object):

    """Counts the progress percentage of a process."""

    def __init__(self, total):
        """Init the counter with the total amout of steps needed to complete the
        process."""
        self.total = total
        self.current = 0.0

    def update(self, increment=1):
        """Increment the number of steps done and prints the percentage."""
        self.current += increment
        sys.stdout.write("\r%s%%" % int(self.current/self.total*100))
        sys.stdout.flush()

    def end(self):
        """Prints the ender 100% (followed by a new line)."""
        sys.stdout.write("\r100%\n")
        sys.stdout.flush()
