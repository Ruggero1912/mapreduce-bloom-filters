package it.unipi.moviesBloomFilters;

public class MovieRow {
    public String movieID;
    public int roundedRating;

    public MovieRow(String movieID, int roundedRating) {
        this.movieID = movieID;
        this.roundedRating = roundedRating;
    }

    public String getMovieID() {
        return movieID;
    }

    public void setMovieID(String movieID) {
        this.movieID = movieID;
    }

    public int getRoundedRating() {
        return roundedRating;
    }

    public void setRoundedRating(int roundedRating) {
        this.roundedRating = roundedRating;
    }
}
