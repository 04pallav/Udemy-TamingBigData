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
		heros = line.split()
		yield heros[0], len(heros)-1

	def reducer(self, heroID, value):
		yield "{:6d}".format(sum(value)), heroID

	def reducer_get_last(self, *args):
		self.last = args

	def mapper_identity(self, *args):
		yield args

	def reducer_final(self):
		count, heroIDs = self.last
		for heroID in heroIDs:
			yield count, heroID


if __name__ == "__main__":
	MRMostPopularSuperhero.run()