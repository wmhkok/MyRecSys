package com.myrecsys.online.datamanager;

import java.util.ArrayList;

/**
 * Embeddingの管理と計算用
 */

public class Embedding {
    ArrayList<Float> emb;

    public Embedding(){
        this.emb = new ArrayList<>();
    }

    public void setEmb(ArrayList<Float> emb){
        this.emb = emb;
    }

    public ArrayList<Float> getEmb(){
        return emb;
    }

    public void addDim(Float element){
        this.emb.add(element);
    }

    public double calcCosineSimilarity(Embedding anotherEmb){
        if (this.emb == null || anotherEmb == null || anotherEmb.getEmb() == null || this.emb.size() != anotherEmb.getEmb().size()){
            return -1;
        }
        double dotProduct = 0;
        double norm1 = 0;
        double norm2 = 0;
        for (int i = 0; i < this.emb.size(); i++){
            dotProduct += this.emb.get(i) * anotherEmb.getEmb().get(i);
            norm1 += Math.pow(this.emb.get(i), 2);
            norm2 += Math.pow(anotherEmb.getEmb().get(i), 2);
        }
        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }   
}



