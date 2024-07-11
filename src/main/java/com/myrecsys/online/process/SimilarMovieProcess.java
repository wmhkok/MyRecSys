package com.myrecsys.online.process;

import java.util.*;

import com.myrecsys.online.datamanager.DataManager;
import com.myrecsys.online.datamanager.Movie;

/**
 * 類似映画リストの取得
 */
public class SimilarMovieProcess {
    public static List<Movie> getSimiMovieList(int movieId, int size) {
        Movie movie = DataManager.getInstance().getMovieById(movieId);
        if (movie == null) {
            return new ArrayList<>();
        }
        List<Movie> candidates = retrievalCandidatesByEmb(movie);

        if (candidates.size() > size) {
            return candidates.subList(0, size);
        }
        return candidates;
    }

    // Embdedingの類似度で映画の候補リストを取得
    public static List<Movie> retrievalCandidatesByEmb(Movie movie) {
        if (movie == null || movie.getEmb() == null) {
            return null;
        }
        
        List<Movie> candidates = DataManager.getInstance().getMovies(10000, "rating");
        HashMap<Movie,Double> movieScoreMap = new HashMap<>();
        for (Movie candidate : candidates){
            double similarity = calculateEmbSimilarScore(movie, candidate);
            movieScoreMap.put(candidate, similarity);
        }

        List<Map.Entry<Movie, Double>> movieScoreList = new ArrayList<>(movieScoreMap.entrySet());
        movieScoreList.sort(Map.Entry.comparingByValue(Collections.reverseOrder()));

        List<Movie> finalCandidates = new ArrayList<>();
        for (Map.Entry<Movie,Double> movieScoreEntry : movieScoreList) {
            finalCandidates.add(movieScoreEntry.getKey());
        }
        return finalCandidates.subList(0, Math.min(finalCandidates.size(), 500));
    }

    // utility
    public static double calculateEmbSimilarScore(Movie movie, Movie candidate){
        if (null == movie || null == candidate){
            return -1;
        }
        return movie.getEmb().calcCosineSimilarity(candidate.getEmb());
    }
    
}