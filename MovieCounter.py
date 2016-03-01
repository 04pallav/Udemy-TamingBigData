from mrjob.job import MRJob

class MRMovieCounter(MRJob):
    def mapper(self, key, line):
        userID, movieID, rating, timestamp = line.split('\t')
        yield int(userID), movieID

    def reducer(self, userID, movieID):
        yield userID, len(list(movieID))

if __name__ == "__main__":
    MRMovieCounter.run()