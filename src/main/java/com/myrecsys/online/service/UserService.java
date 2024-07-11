package com.myrecsys.online.service;

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myrecsys.online.datamanager.DataManager;
import com.myrecsys.online.datamanager.User;

public class UserService extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            String userId = request.getParameter("id");
            User user = DataManager.getInstance().getUserById(Integer.parseInt(userId));

            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");

            if (user != null) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonUser = mapper.writeValueAsString(user);
                response.getWriter().println(jsonUser);
            } else {
                response.getWriter().println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println("");
        }
    }
}