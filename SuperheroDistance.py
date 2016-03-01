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
        try:
            heroID, tail = line.split(None, 1)
        except ValueError:
            heroID = line
            tail = ""

        heroID = int(heroID)

        if tail and tail[0] == '"':
            # assume this is an heroID, heroName mapping
            yield heroID, dict(heroID=heroID, name=unicode(tail[1:-1],'latin1'))
        else:
            companions = [int(i) for i in tail.split()]

            if heroID == self.options.target:
                yield heroID, dict(heroID=heroID, companions=companions, color=GRAY, distance=0)
            else:
                yield heroID, dict(heroID=heroID, companions=companions, color=WHITE, distance=INFINITY)


class MRBFSStep(MRJob):
    """
        Map-reduce implementation of the breadth first algorithm
    """
    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

    def mapper(self, key, hero):
        if hero.get('color') == GRAY:
            siblingDistance = dict(color=GRAY, distance=hero['distance']+1)
            for companion in hero['companions']:
                yield companion, siblingDistance
                if companion == 100:
                    self.increment_counter("superhero","found")
            hero['color'] = BLACK
            self.increment_counter("superhero","touched")

        # In all cases emit the hero
        yield hero['heroID'], hero



    def reducer(self, heroID, records):
        # Merge records
        hero = dict(heroID=heroID, color=WHITE, distance=INFINITY, companions=[])
        for record in records:
            newColor = max(hero['color'], record.get('color',WHITE))
            newDistance = min(hero['distance'], record.get('distance',INFINITY))
            newCompanions = hero['companions'] + record.get('companions', [])

            hero.update(record)
            hero.update(dict(color=newColor, distance=newDistance, companions=newCompanions))

        yield heroID, hero


class MRResult(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mapper(self, key, hero):
        if hero['color'] > WHITE:
            yield key, dict(heroID=hero['heroID'], name=hero['name'], distance=hero['distance'], color=hero['color'])

