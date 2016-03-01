from mrjob.job import MRJob
from mrjob.step import MRStep

import mrjob.protocol

HULK = 2548

WHITE = 0
GRAY = 1
BLACK = 2

INFINITY = 9999

class MRSuperheroDistance(MRJob):
	"""
		Map-reduce implementation of the breadth first algorithm
	"""
	def __init__(self, *args, **kwargs):
		super(MRSuperheroDistance, self).__init__(*args, **kwargs)

	def configure_options(self):
	    super(MRSuperheroDistance, self).configure_options()
	    self.add_passthrough_option(
	        '--target', default=HULK, type=int, help='Your target superhero ID')


	def steps(self):
			return [
				MRStep(mapper=self.pre_process),
				MRStep(mapper=self.mapper_bsf_iteration, reducer=self.reducer_bsf_iteration),
			#	MRStep(mapper=self.mapper_identity, # mrjob 0.4.6 bug : https://github.com/Yelp/mrjob/issues/1141
			#		   reducer=self.reducer_get_last, reducer_final=self.reducer_final)
			]

	def pre_process(self, key, line):
		fields = [int(i) for i in line.split()]
		heroID=fields[0]

		if fields[0] == self.options.target:
			yield heroID, dict(heroID=heroID, companions=fields[1:], color=GRAY, distance=0)
		else:
			yield heroID, dict(heroID=heroID, companions=fields[1:], color=WHITE, distance=INFINITY)

	def mapper_bsf_iteration(self, key, hero):
		if hero['color'] == GRAY:
			siblingDistance = dict(color=GRAY, distance=hero['distance']+1)
			for companion in hero['companions']:
				yield companion, siblingDistance
			hero['color'] = BLACK

		# In all cases emit the hero
		yield hero['heroID'], hero



	def reducer_bsf_iteration(self, heroID, records):
		# Merge records
		hero = dict(heroID=heroID, color=WHITE, distance=INFINITY, companions=[])
		for record in records:
			hero['companions'] += record.get('companions', [])
			hero['color'] = max(hero['color'], record['color'])
			hero['distance'] = min(hero['distance'], record['distance'])

		yield heroID, hero

	def reducer_get_last(self, *args):
		self.last = args

	def mapper_identity(self, *args):
		yield args

	def reducer_final(self):
		count, records = self.last
		for record in records:
			yield count, record


if __name__ == "__main__":
	MRSuperheroDistance.run()