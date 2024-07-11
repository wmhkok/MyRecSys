package com.myrecsys.online.service;

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myrecsys.online.datamanager.DataManager;
import com.myrecsys.online.datamanager.Movie;


public class MovieService extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            String moiveId = request.getParameter("id");
            Movie movie = DataManager.getInstance().getMovieById(Integer.parseInt(moiveId));
            
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");

            if (movie != null) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonMovie = mapper.writeValueAsString(movie);
                response.getWriter().println(jsonMovie);
            }else {
                response.getWriter().println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}
