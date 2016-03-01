from mrjob.job import MRJob
from mrjob.step import MRStep

import mrjob.protocol


class MRMostPopularMovie(MRJob):
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def __init__(self, *args, **kwargs):
        super(MRMostPopularMovie, self).__init__(*args, **kwargs)
        self.last=(0,[])

    def steps(self):
            return [
                MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(mapper=self.mapper_identity, # mrjob 0.4.6 bug : https://github.com/Yelp/mrjob/issues/1141
                       reducer=self.reducer_get_last, reducer_final=self.reducer_final)
            ]
    def mapper(self, key, line):
        userID, movieID, rating, timestamp = line.split('\t')
        yield movieID, 1

    def reducer(self, movieID, value):
        yield "{:6d}".format(sum(value)), movieID

    def reducer_get_last(self, *args):
        self.last = args

    def mapper_identity(self, *args):
        yield args

    def reducer_final(self):
        count, movieIDs = self.last
        for movieID in movieIDs:
            yield count, movieID


if __name__ == "__main__":
    MRMostPopularMovie.run()