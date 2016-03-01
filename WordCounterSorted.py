from mrjob.job import MRJob
from mrjob.step import MRStep

import re

WORD_REGEXP = re.compile(r"[^\W\d_]+", re.UNICODE)

class MRWordCounter(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.mapper_get_words,
                        reducer=self.reduce_count_words),
                MRStep(mapper=self.mapper_make_count_key,
                        reducer=self.reduce_output_words),
            ]
    def mapper_get_words(self, key, line):
        for word in WORD_REGEXP.findall(unicode(line, "UTF8").lower()):
            yield word, 1

    def reduce_count_words(self, word, count):
        yield word, sum(count)

    def mapper_make_count_key(self, word, count):
        yield "{:05d}".format(count), word

    def reduce_output_words(self, count, words):
        for word in words:
            yield count, word



if __name__ == "__main__":
    MRWordCounter.run()
