/* loading the u.data into ratings relantio */
ratings = LOAD '/user/deepjyotiroy079/ml-100k/u.data' AS
        (userID: int, movieID: int, rating: int, ratingTime: int);

/* loading metadata */
metadata = LOAD '/user/deepjyotiroy079/ml-100k/u.item' USING PigStorage('|')
                AS (movieID: int, movieTitle: chararray, releaseDate: int, videoRelease: chararray, ImdbLink: chararray);

/*  */
namesLookup = FOREACH metadata GENERATE movieID, movieTitle, ToDate(releaseDate) AS releaseTime;

ratingsByMovieId = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovieId GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

topFiveMovies = FILTER avgRatings BY avgRating > 4;

joinMovies = JOIN topFiveMovies BY movieID, namesLookup BY movieID;

oldestFiveStarMovies = ORDER joinMovies BY namesLookup::releaseTime;

dump oldestFiveStarMovies;