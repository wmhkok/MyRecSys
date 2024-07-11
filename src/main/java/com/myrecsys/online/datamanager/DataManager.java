package com.myrecsys.online.datamanager;

import java.util.*;

import java.io.File;

public class DataManager {
    // シングルトンインスタンス
    private static volatile DataManager instance;
    Map<Integer, Movie> movieMap;
    Map<Integer, User> userMap;
    Map<String, List<Movie>> genreReverseIndexMap;

    private DataManager() {
        this.movieMap = new HashMap<>();
        this.userMap = new HashMap<>();
        this.genreReverseIndexMap = new HashMap<>();
        instance = this;
    }

    public static DataManager getInstance() {
        if (instance == null) {
            synchronized (DataManager.class) {
                if (instance == null) {
                    instance = new DataManager();
                }
            }
        }
        return instance;
    }

    public void loadData(String movieDataPath, String linkDataPath, String ratingDataPath, String movieEmbPath, String userEmbPath, String movieRedisKey, String userRedisKey) throws Exception {
        loadMovieData(movieDataPath);
        loadLinkData(linkDataPath);
        loadRatingData(ratingDataPath);
        loadMovieEmb(movieEmbPath, movieRedisKey);
        loadUserEmb(userEmbPath, userRedisKey);
        //loadMovieFeatures("mf:");
    }

    private void loadMovieData(String movieDataPath) throws Exception {
        System.out.println("loading movie data from" + movieDataPath + "...");
        boolean skipFirstLine = true;
        try (Scanner scanner = new Scanner(new File(movieDataPath))) {
            while (scanner.hasNextLine()) {
                String movieRawData = scanner.nextLine();
                if (skipFirstLine) {
                    skipFirstLine = false;
                    continue;
                }
                String[] movieData = movieRawData.split(",");
                if (movieData.length == 3) {
                    Movie movie = new Movie();
                    movie.setMovieId(Integer.parseInt(movieData[0]));
                    int releaseYear = extractReleaseYear(movieData[1].trim());
                    if (releaseYear == -1) {
                        movie.setTitle(movieData[1].trim());
                    } else {
                        movie.setReleaseYear(releaseYear);
                        movie.setTitle(movieData[1].trim().substring(0, movieData[1].trim().length()-6).trim());
                    }
                    String genres = movieData[2];
                    if (!genres.trim().isEmpty()) {
                        String[] genreArray = genres.split("\\|");
                        for (String genre : genreArray) {
                            movie.addGenre(genre);
                            addMovie2GenreIndex(genre, movie);
                        }
                    }
                    this.movieMap.put(movie.getMovieId(), movie);
                }
            }
        }
        System.out.println("Loading movie data completed. " + this.movieMap.size() + " movies in total.");
    }

    

    private void loadLinkData(String linkDataPath) throws Exception {
        System.out.println("Loading link data from " + linkDataPath + " ...");
        int count = 0;
        boolean skipFirstLine = true;
        try (Scanner scanner = new Scanner(new File(linkDataPath))) {
            while (scanner.hasNextLine()) {
                String linkRawData = scanner.nextLine();
                if (skipFirstLine) {
                    skipFirstLine = false;
                    continue;
                }
                String[] linkData = linkRawData.split(",");
                if (linkData.length == 3) {
                    int movieId = Integer.parseInt(linkData[0]);
                    Movie movie = this.movieMap.get(movieId);
                    if (movie != null) {
                        count ++;
                        movie.setImdbId(linkData[1].trim());
                        movie.setTmdbId(linkData[2].trim());
                    }
                }
            }
        }
        System.out.println("Loading link data completed. " + count + " links in total.");
    }

    private void loadRatingData(String ratingDataPath) throws Exception {
        System.out.println("Loading rating data from " + ratingDataPath + " ...");
        boolean skipFirstLine = true;
        int count = 0;
        try (Scanner scanner = new Scanner(new File(ratingDataPath))) {
            while (scanner.hasNextLine()) {
                String ratingRawData = scanner.nextLine();
                if (skipFirstLine) {
                    skipFirstLine = false;
                    continue;
                }
                String[] ratingData = ratingRawData.split(",");
                if (ratingData.length == 4) {
                    count ++;
                    Rating rating = new Rating();
                    rating.setUserId(Integer.parseInt(ratingData[0]));
                    rating.setMovieId(Integer.parseInt(ratingData[1]));
                    rating.setScore(Float.parseFloat(ratingData[2]));
                    rating.setTimestamp(Long.parseLong(ratingData[3]));
                    Movie movie = this.movieMap.get(rating.getMovieId());
                    if (movie != null) {
                        movie.addRating(rating);
                    }
                    if (!this.userMap.containsKey(rating.getUserId())) {
                        User user = new User();
                        user.setUserId(rating.getUserId());
                        this.userMap.put(user.getUserId(), user);
                    }
                    this.userMap.get(rating.getUserId()).addRating(rating);
                }
            }
        }
        System.out.println("Loading rating data completed. " + count + " ratings in total.");
    }

    static final String DATA_SOURCE = "file";

    private void loadMovieEmb(String movieEmbPath, String embKey) throws Exception {
        if (DATA_SOURCE.equals("file")) {
            System.out.println("Loading movie embedding from " + movieEmbPath + " ...");
            int validEmbCount = 0;
            try (Scanner scanner = new Scanner(new File(movieEmbPath))) {
                while (scanner.hasNextLine()) {
                    String movieRawEmbData = scanner.nextLine();
                    String[] movieEmbData = movieRawEmbData.split(":");
                    if (movieEmbData.length == 2) {
                        Movie m = getMovieById(Integer.parseInt(movieEmbData[0]));
                        if (m == null) {
                            continue;
                        }
                        m.setEmb(parseEmbStr(movieEmbData[1]));
                        validEmbCount++;
                    }
                }
            }
            System.out.println("Loading movie embedding completed. " + validEmbCount + " movie embeddings in total.");
        } else {
            System.out.println("Loading movie embedding from Redis ...");
            Set<String> movieEmbKeys = RedisClient.getInstance().keys(embKey + "*");
            int validEmbCount = 0;
            for (String movieEmbKey : movieEmbKeys){
                String movieId = movieEmbKey.split(":")[1];
                Movie m = getMovieById(Integer.parseInt(movieId));
                if (m == null) {
                    continue;
                }
                m.setEmb(parseEmbStr(RedisClient.getInstance().get(movieEmbKey)));
                validEmbCount++;
            }
            System.out.println("Loading movie embedding completed. " + validEmbCount + " movie embeddings in total.");
        }

    }

    private void loadUserEmb(String userEmbPath, String embKey) throws Exception {
        if (DATA_SOURCE.equals("file")) { 
            System.out.println("Loading user embedding from" + userEmbPath + "...");
            int validEmbCount = 0;
            try (Scanner scanner = new Scanner(new File(userEmbPath))) {
                while (scanner.hasNextLine()) {
                    String userRawEmbData = scanner.nextLine();
                    String[] userEmbData = userRawEmbData.split(":");
                    if (userEmbData.length == 2) {
                        User u = getUserById(Integer.parseInt(userEmbData[0]));
                        if (u == null) {
                            continue;
                        }
                        u.setEmb(parseEmbStr(userEmbData[1]));
                        validEmbCount ++;
                    }
                }
            }
            System.out.println("Loading user embedding completed. " + validEmbCount + " user embeddings in total.");
        } else {
            System.out.println("Loading movie embedding from Redis ...");
        }
    }

    private void loadMovieFeatures(String movieFeaturesPrefix) throws Exception {
        System.out.println("Loading movie features from Redis ...");
        Set<String> movieFeaturesKeys = RedisClient.getInstance().keys(movieFeaturesPrefix + "*");
        int validFeatureCount = 0;
        for (String movieFeaturesKey : movieFeaturesKeys) {
            String movieId = movieFeaturesKey.split(":")[1];
            Movie m = getMovieById(Integer.parseInt(movieId));
            if (m == null) {
                continue;
            }
            m.setMovieFeatures(RedisClient.getInstance().hgetAll(movieFeaturesKey));
            validFeatureCount ++;
        }
        System.out.println("Loading movie features completed. " + validFeatureCount + " movie features in total.");
    }

    private int extractReleaseYear(String rawTitle) {
        rawTitle = rawTitle.trim();
        if (rawTitle == null || rawTitle.length() < 6) {
            return -1;
        } else {
            String yearString = rawTitle.substring(rawTitle.length()-5, rawTitle.length()-1);
            try {
                return Integer.parseInt(yearString);
            } catch (NumberFormatException exception) {
                return -1;
            }
        }
    }

    private Embedding parseEmbStr(String embStr){
        String[] embStrings = embStr.split("\\s");
        Embedding emb = new Embedding();
        for (String element : embStrings) {
            emb.addDim(Float.parseFloat(element));
        }
        return emb;
    }

    private void addMovie2GenreIndex(String genre, Movie movie) {
        if (!this.genreReverseIndexMap.containsKey(genre)) {
            this.genreReverseIndexMap.put(genre, new ArrayList<>());
        }
        this.genreReverseIndexMap.get(genre).add(movie);
    }

    public Movie getMovieById(int movieId) {
        return this.movieMap.get(movieId);
    }

    public User getUserById(int userId) {
        return this.userMap.get(userId);
    }

    public List<Movie> getMovieByGenre(String genre, Integer maxSize, String sortBy) {
        if (genre != null && genre != "") {
            List<Movie> movies = new ArrayList<>(this.genreReverseIndexMap.get(genre));
            switch (sortBy) {
                case "rating":
                    movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating())); 
                    break;
                case "releaseYear":
                    movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear())); 
                    break;
                default:
            }

            if (movies.size() > maxSize) {
                return movies.subList(0, maxSize);
            }
            return movies;
        }
        return null;
    }

    public List<Movie> getAllMovies() {
        return new ArrayList<>(movieMap.values());
    }

    public List<Movie> getMovies(int maxSize, String sortBy){
        List<Movie> movies = new ArrayList<>(movieMap.values());
        switch (sortBy) {
            case "rating":
                movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating())); 
                break;
            case "releaseYear": 
                movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear())); 
                break;
            default:
        }

        if (movies.size() > maxSize){
            return movies.subList(0, maxSize);
        }
        return movies;
    }    
}
