package com.myrecsys.online.process;

import com.myrecsys.online.datamanager.DataManager;
import com.myrecsys.online.datamanager.Movie;
import com.myrecsys.online.datamanager.User;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;


import static com.myrecsys.online.util.ModelHttpClient.asyncSinglePostRequest;

public class RecForYouProcess {
    public static List<Movie> getRecList(int userId, int size) {
        User user = DataManager.getInstance().getUserById(userId);
        if (user == null) {
            return new ArrayList<>();
        }
        List<Movie> candidates = retrievalCandidatesByEmb(user);

        List<Movie> rankedList = ranker(user, candidates);

        if (rankedList.size() > size) {
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    public static List<Movie> retrievalCandidatesByEmb(User user) {
        if (user == null || user.getEmb() == null) {
            return null;
        }

        List<Movie> candidates = DataManager.getInstance().getMovies(1000, "rating");
        HashMap<Movie,Double> movieScoreMap = new HashMap<>();

        for (Movie candidate : candidates){
            double similarity = calculateEmbSimilarScore(user, candidate);
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

    public static List<Movie> ranker(User user, List<Movie> candidates) {
        HashMap<Movie, Double> candidateScoreMap = callNeuralCFServing(user, candidates);
        if (candidateScoreMap.isEmpty()) {
            return candidates;
        }
        
        List<Map.Entry<Movie, Double>> candidateScoreList = new ArrayList<>(candidateScoreMap.entrySet());
        candidateScoreList.sort(Map.Entry.comparingByValue(Collections.reverseOrder()));

        List<Movie> rankedList = new ArrayList<>();
        for (Map.Entry<Movie,Double> movieScoreEntry : candidateScoreList) {
            rankedList.add(movieScoreEntry.getKey());
        }
        return rankedList;
    }

    public static HashMap<Movie, Double> callNeuralCFServing(User user, List<Movie> candidates) {
        if (user == null || candidates == null || candidates.isEmpty()) {
            return new HashMap<>();
        }
        JSONArray instances = new JSONArray();
        for (Movie movie : candidates) {
            JSONObject instance = new JSONObject();
            instance.put("userId", user.getUserId());
            instance.put("movieId", movie.getMovieId());
            instances.put(instance);
        }

        String predictions = asyncSinglePostRequest("http://localhost:8080/predictions/neuralCF", instances.toString());
        /*
            * response format
            * {"data": [0.6646174192428589, 0.3985872268676758, ...]} or ""
        */
        if (!predictions.equals("")) {
            System.out.println("send user" + user.getUserId() + " request to torch serve.");

            JSONObject predictionsObject = new JSONObject(predictions);
            System.out.println(predictions);
            JSONArray scores = predictionsObject.getJSONArray("data");

            HashMap<Movie, Double> candidateScoreMap =new HashMap<>();
            for (int i = 0; i < candidates.size(); i++) {
                candidateScoreMap.put(candidates.get(i), scores.getDouble(i));
            }
            return candidateScoreMap;
        } else {
            return new HashMap<>();
        }
        
    }

    // utility
    public static double calculateEmbSimilarScore(User user, Movie candidate){
        if (null == user || null == candidate){
            return -1;
        }
        return user.getEmb().calcCosineSimilarity(candidate.getEmb());
    }

}