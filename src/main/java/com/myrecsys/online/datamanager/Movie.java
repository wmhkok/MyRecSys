package com.myrecsys.online.datamanager;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * movies.csv, ratings.csv, links.csvから読み込まれたデータを保有
 */

public class Movie {
    int movieId;
    String title;
    int releaseYear;
    String imdbId;
    String tmdbId;
    List<String> genres;
    // この映画のレビュー数
    int ratingNumber;
    double averageRating;

    @JsonIgnore
    Embedding emb;

    @JsonIgnore
    List<Rating> ratings;

    @JsonIgnore
    Map<String, String> movieFeatures;

    final int TOP_RATING_SIZE = 10;
    @JsonSerialize(using = RatingListSerializer.class)
    List<Rating> topRatings;

    public Movie() {
        this.ratingNumber = 0;
        this.averageRating = 0;
        this.genres = new ArrayList<>();
        this.ratings = new ArrayList<>();
        this.topRatings = new LinkedList<>();
        this.emb = null;
        this.movieFeatures = null;
    }
    
    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public void addGenre(String genre) {
        this.genres.add(genre);
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }

    public List<String> getGenres() {
        return genres;
    }

    public List<Rating> getRatings() {
        return ratings;
    }

    public void addRating(Rating rating) {
        this.averageRating = (this.averageRating * this.ratingNumber + rating.getScore()) / (this.ratingNumber+1);
        this.ratingNumber++;
        this.ratings.add(rating);
        addTopRating(rating);
    }

    public void addTopRating(Rating rating) {
        if (this.topRatings.isEmpty()){
            this.topRatings.add(rating);
        } else {
            int index = 0;
            for (Rating topRating : this.topRatings) {
                if (topRating.getScore() >= rating.getScore()) {
                    break;
                }
                index ++;
            }
            topRatings.add(index, rating);
            if (topRatings.size() > TOP_RATING_SIZE) {
                topRatings.remove(0);
            }
        }
    }

    public String getImdbId() {
        return imdbId;
    }

    public void setImdbId(String imdbId) {
        this.imdbId = imdbId;
    }

    public String getTmdbId() {
        return tmdbId;
    }

    public void setTmdbId(String tmdbId) {
        this.tmdbId = tmdbId;
    }

    public int getRatingNumber() {
        return ratingNumber;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public Embedding getEmb() {
        return emb;
    }

    public void setEmb(Embedding emb) {
        this.emb = emb;
    }

    public Map<String, String> getMovieFeatures() {
        return movieFeatures;
    }

    public void setMovieFeatures(Map<String, String> movieFeatures) {
        this.movieFeatures = movieFeatures;
    }
}
