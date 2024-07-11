package com.myrecsys.online.util;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class ModelHttpClient {

    public static String asyncSinglePostRequest(String host, String body) {
        if (null == body || body.isEmpty()) {
            return "";
        }

        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(host))
                    .header("Content-Type", "application/json")
                    .POST(BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();

            CompletableFuture<HttpResponse<String>> future = client.sendAsync(request, BodyHandlers.ofString(StandardCharsets.UTF_8));

            HttpResponse<String> response = future.join();
            return response.body();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}