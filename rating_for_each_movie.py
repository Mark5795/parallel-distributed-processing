from mrjob.job import MRJob
from mrjob.step import MRStep

# Count the number of ratings given for each movie. 

class RatingsForEachMovie (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                    reducer=self.reducer_count_ratings)
        ]

# Select the movie_id in the file
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

# Count how many times a movie_id is rated.
    def reducer_count_ratings (self, key, values):
        yield key , sum(values)


if __name__ == '__main__':
    RatingsForEachMovie.run()