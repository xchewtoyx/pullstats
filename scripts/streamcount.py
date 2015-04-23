#!/usr/bin/env python

import json
import sys

#{
#    results: [
#        {name: name, length: length},
#    ]
#}
def stream_stats(results):
    for result in results:
        yield result['stream']['name'], result['stream']['length']

def main():
    streams = json.load(sys.stdin)
    influx = []
    influx.append({
        'name': 'stream_count',
        'columns': ['stream', 'unread'],
        'points': list(stream_stats(streams['results']))
    })
    print json.dumps(influx)


if __name__ == '__main__':
    main()
