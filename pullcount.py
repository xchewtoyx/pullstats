#!/usr/bin/env python

import json
import sys

def main():
    counts = json.load(sys.stdin)
    influx = {
        'name': 'pull_counts',
        'columns': ['read', 'new', 'unread', 'total'],
        'points': [[
            counts['counts']['read'],
            counts['counts']['new'],
            counts['counts']['unread'],
            counts['counts']['total'],
        ]]
    }
    print json.dumps([influx])


if __name__ == '__main__':
    main()
