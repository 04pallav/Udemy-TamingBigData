from mrjob.job import MRJob

from mrjob.step import MRStep

class MRMostRatedMovie(MRJob):
	def steps(self):
			return [
				MRStep(mapper=self.mapper, reducer=self.reducer),
				MRStep(reducer=self.reducer_get_max, reducer_final=self.reducer_final)
			]
	def mapper(self, key, line):
		userID, movieID, rating, timestamp = line.split('\t')
		yield movieID, 1

	def reducer(self, movieID, value):
		yield sum(value), movieID

	def reducer_get_max(self, *args):
		self.last = args

	def reducer_final(self):
		count, movieIDs = self.last
		for movieID in movieIDs:
			yield count, movieID


if __name__ == "__main__":
	MRMostRatedMovie.run()