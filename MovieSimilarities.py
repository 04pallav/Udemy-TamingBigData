from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

import itertools

class MRMovieSimilarities(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.mapper_parse_input,
                        reducer=self.reduce_ratings_by_user),
                MRStep(mapper=self.mapper_make_pairs,
                        reducer=self.reducer_compute_similarities),
                MRStep(mapper=self.mapper_sort_similarities,
                        reducer=self.reducer_output_similarities),
            ]

    def mapper_parse_input(self, _, line):
        userID, movieID, rating, timestamp = line.split('\t')
        yield userID, (movieID, float(rating))


    def reduce_ratings_by_user(self, userID, itemRatings):
        yield userID, list(itemRatings) # is this required ?

    def mapper_make_pairs(self, userID, itemRatings):
        for ir1, ir2 in itertools.combinations(itemRatings, 2):
            yield (ir1[0],ir2[0]),(ir1[1],ir2[1]) # Could use zip() instead
            yield (ir2[0],ir1[0]),(ir2[1],ir1[1])

    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarities(self, moviePair, ratingPairs):
        score, numPairs = self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (numPairs > 10 and score > 0.95):
            yield moviePair, (score, numPairs)        

    def mapper_sort_similarities(self, moviePair, scores):
        # Shuffle things around so the key is (movie1, score)
        # so we have meaningfully sorted results.
        score, n = scores
        movie1, movie2 = moviePair

        yield (movie1, score), (movie2, n)

    def reducer_output_similarities(self, movieScore, similarN):
        # Output the results.
        # Movie => Similar Movie, score, number of co-ratings
        movie1, score = movieScore
        for movie2, n in similarN:
            yield movie1, (movie2, score, n)



if __name__ == "__main__":
    MRMovieSimilarities.run()
