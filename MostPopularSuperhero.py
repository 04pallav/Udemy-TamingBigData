from mrjob.job import MRJob
from mrjob.step import MRStep

import mrjob.protocol


class MRMostPopularSuperhero(MRJob):
    def __init__(self, *args, **kwargs):
        super(MRMostPopularSuperhero, self).__init__(*args, **kwargs)
        self.last=(0,[])

    def steps(self):
            return [
                MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(mapper=self.mapper_identity, # mrjob 0.4.6 bug : https://github.com/Yelp/mrjob/issues/1141
                       reducer=self.reducer_get_last, reducer_final=self.reducer_final)
            ]
    def mapper(self, key, line):
        try:
            heroID, tail = line.split(None, 1)
        except ValueError:
            heroID = line
            tail = ""

        if tail and tail[0] == '"':
            # assume this is an heroID, heroName mapping
            yield heroID, { 'name' : unicode(tail[1:-1],'latin1') }
        else:
            # assume this is a graph connection
            yield heroID, { 'count' : len(tail.split()) }

    def reducer(self, heroID, cols):
        record = { 'heroID' : heroID, 'count' : 0 }
        for col in cols: # merge records
            count = record['count'] + col.get('count', 0)
            record.update(col)
            record['count'] = count

        yield "{:6d}".format(record['count']), record

    def reducer_get_last(self, *args):
        self.last = args

    def mapper_identity(self, *args):
        yield args

    def reducer_final(self):
        count, records = self.last
        for record in records:
            yield count, record


if __name__ == "__main__":
    MRMostPopularSuperhero.run()