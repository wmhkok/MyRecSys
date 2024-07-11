package com.myrecsys.online.service;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myrecsys.online.datamanager.Movie;
import com.myrecsys.online.process.RecForYouProcess;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class RecForYouService extends HttpServlet{
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException ,IOException {
        try {
            String userId = request.getParameter("id");
            String size = request.getParameter("size");
            
            List<Movie> movies = RecForYouProcess.getRecList(Integer.parseInt(userId), Integer.parseInt(size));

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
