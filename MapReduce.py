from mrjob.job import MRJob
from mrjob.step import MRStep


class CountRatingsPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_by_count)
        ]

    def mapper_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        # Each row in the dataset is 1 rating. So all we have to do is count the number of rows with the same movieID.
        # If we yield movieID, 1, then the reducer can sum those 1's to get the number of ratings for that movie.
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        # Yield the key and value as a tuple with no key, so we can sort them together in the next reducer
        yield None, (sum(values), key)

    def reducer_sort_by_count(self, _, rating_counts):
        # Sort the rating_counts by the count of ratings, descending
        for count, key in sorted(rating_counts, reverse=True):
            yield (int(count), key)


if __name__ == '__main__':
    CountRatingsPerMovie.run()