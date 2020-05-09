from mrjob.job import MRJob
from mrjob.step import MRStep

# Count the number of ratings given for each movie. 

class SortMoviesByNumberOfRatings (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                    reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_counts)
        ]

# Select the movie_id in the file
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

# Count how many times a movie_id is rated.
    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

# Sorts the movie_id based on their numbers of rating  
    def reducer_sort_counts(self, _, rating_counts):
        for count, key in sorted(rating_counts, reverse=True):
            yield (int(count), key)


if __name__ == '__main__':
    SortMoviesByNumberOfRatings.run()