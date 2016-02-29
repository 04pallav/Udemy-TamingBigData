from mrjob.job import MRJob
import mrjob.protocol


import re

WORD_REGEXP = re.compile(r"[^\W\d_]+", re.UNICODE)

class MRWordCounter(MRJob):
	OUTPUT_PROTOCOL = mrjob.protocol.ReprProtocol
	INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol


	def mapper(self, key, line):
		for word in WORD_REGEXP.findall(unicode(line, "UTF8").lower()):
			yield word, 1

	def combiner(self, word, count):
		yield word, sum(count)

	def reducer(self, word, count):
		yield word, sum(count)

#	combiner = reducer


if __name__ == "__main__":
	MRWordCounter.run()
