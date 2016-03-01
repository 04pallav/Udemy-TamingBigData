from mrjob.job import MRJob
from mrjob.step import MRStep

import mrjob.protocol

HULK = 2548

WHITE = 0
GRAY = 1
BLACK = 2

INFINITY = 9999

class MRPreProcess(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

    def configure_options(self):
        super(MRPreProcess, self).configure_options()
        self.add_passthrough_option(
            '--target', default=HULK, type=int, help='Your target superhero ID')

    def mapper(self, key, line):
        fields = [int(i) for i in line.split()]
        heroID=fields[0]

        if fields[0] == self.options.target:
            yield heroID, dict(heroID=heroID, companions=fields[1:], color=GRAY, distance=0)
        else:
            yield heroID, dict(heroID=heroID, companions=fields[1:], color=WHITE, distance=INFINITY)

class MRBFSStep(MRJob):
    """
        Map-reduce implementation of the breadth first algorithm
    """
    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

    def mapper(self, key, hero):
        if hero['color'] == GRAY:
            siblingDistance = dict(color=GRAY, distance=hero['distance']+1)
            for companion in hero['companions']:
                yield companion, siblingDistance
                if companion == 10000:
                    self.increment_counter("superhero","found")
            hero['color'] = BLACK
            self.increment_counter("superhero","touched")

        # In all cases emit the hero
        yield hero['heroID'], hero



    def reducer(self, heroID, records):
        # Merge records
        hero = dict(heroID=heroID, color=WHITE, distance=INFINITY, companions=[])
        for record in records:
            hero['companions'] += record.get('companions', [])
            hero['color'] = max(hero['color'], record['color'])
            hero['distance'] = min(hero['distance'], record['distance'])

        yield heroID, hero


class MRResult(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mapper(self, key, hero):
        if hero['color'] > WHITE:
            yield key, dict(heroID=hero['heroID'], distance=hero['distance'], color=hero['color'])

