package com.myrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myrecsys.online.datamanager.DataManager;
import com.myrecsys.online.datamanager.Movie;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

public class RecommendationService extends HttpServlet{
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            String genre = request.getParameter("genre");
            String size = request.getParameter("size");
            String sortby = request.getParameter("sortby");

            List<Movie> movies = DataManager.getInstance().getMovieByGenre(genre, Integer.parseInt(size), sortby);
            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(movies);

            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().println(jsonMovies);

        } catch (Exception e){
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}