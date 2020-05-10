from mrjob.job import MRJob
from mrjob.step import MRStep


class SortGenresByTotalRatings(MRJob):

    # To add the second data file
    def configure_args(self):
        super(SortGenresByTotalRatings, self).configure_args()
        self.add_file_arg('--genres')

    def steps(self):
        return [
                    MRStep(mapper = self.mapper_get_genres,
                            combiner=self.combiner_count_ratings,
                            reducer = self.reducer_merge_genres),
                    MRStep(reducer = self.reducer_sort_counts, 
                            reducer_init = self.mapper_init)
               ]
    
    # Reads second data file and get the genre
    def mapper_init(self):
        self.genre_names = []

        with open("u.genre") as genreFile:
            for line in genreFile:
                genre = line.split('|')
                self.genre_names.append(genre[0]) 

    # Get the genres for each movie.
    # Put them in a tuple.
    # Iterate the tuple and add a key to the value.
    def mapper_get_genres(self, _, line):
        (movieID, movieName, movieDate, empty, movieURL, movieGenres) = line.split('|', 5)
        genres = (genre0, genre1, genre2, genre3, genre4, genre5, genre6, genre7, genre8, genre9, 
        genre10, genre11, genre12, genre13, genre14, genre15, genre16, genre17, genre18) = movieGenres.split('|')

        i = 0
        for genre in genres:            
            yield (i, int(genre))
            i += 1

    # Count the ratings for the genres
    def combiner_count_ratings(self, key, values):
        yield (key, sum(values))

    # Merge the genres
    def reducer_merge_genres(self, key, values):
         yield None, (sum(values), key)

    # Sorts the genres based on their numbers of ratings 
    def reducer_sort_counts(self, _, rating_counts):
        for count, key in sorted(rating_counts, reverse=True):
            yield (int(count), self.genre_names[key])       

if __name__ == '__main__':
    SortGenresByTotalRatings.run()