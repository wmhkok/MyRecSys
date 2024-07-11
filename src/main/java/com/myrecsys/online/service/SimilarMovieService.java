package com.myrecsys.online.service;

import java.io.IOException;
import java.util.*;

import com.myrecsys.online.process.SimilarMovieProcess;
import com.myrecsys.online.datamanager.Movie;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;


public class SimilarMovieService extends HttpServlet{
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException ,IOException {
        try {
            String movieId = request.getParameter("movieId");
            String size = request.getParameter("size");
            
            List<Movie> movies = SimilarMovieProcess.getSimiMovieList(Integer.parseInt(movieId), Integer.parseInt(size));

            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");

            ObjectMapper mapper = new ObjectMapper();
            String jsonMovies = mapper.writeValueAsString(movies);
            response.getWriter().println(jsonMovies);
            
        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
