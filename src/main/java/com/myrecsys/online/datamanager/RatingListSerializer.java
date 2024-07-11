package com.myrecsys.online.datamanager;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class RatingListSerializer extends JsonSerializer<List<Rating>> {
    
    @Override
    public void serialize(List<Rating> ratingList, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartArray();
        for (Rating rating : ratingList) {
            gen.writeStartObject();
            gen.writeObjectField("rating", rating);
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }
    
}
