from mrjob.job import MRJob

import re

WORD_REGEXP = re.compile(r"[^\W\d_]+", re.UNICODE)

class MRWordCounter(MRJob):
    def mapper(self, key, line):
        for word in WORD_REGEXP.findall(unicode(line, "UTF8").lower()):
            yield word, 1

    def reducer(self, word, count):
        yield word, sum(count)

if __name__ == "__main__":
    MRWordCounter.run()
