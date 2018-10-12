package com.shabha.app.grid.model;

import java.util.List;

import scala.Tuple2;

public class ModelMetrics {

    private String product;
    private String version;
    private String name;
    private double accuracy;
    private double areaUnderROC;
    private double areaUnderPRC;
    private List<Double> thresholds;
    private List<Tuple2<Double, Double>> precision;
    private List<Tuple2<Double, Double>> recall;
    private List<Tuple2<Double, Double>> f1Score;
    private List<Tuple2<Double, Double>> f2Score;
    private List<Tuple2<Double, Double>> prc;
    private List<Tuple2<Double, Double>> roc;
    
  
    public String getProduct() {
        return product;
    }
    
    public void setProduct(String product) {
        this.product = product;
    }

    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    public double getAreaUnderROC() {
        return areaUnderROC;
    }

    public void setAreaUnderROC(double areaUnderROC) {
        this.areaUnderROC = areaUnderROC;
    }

    public double getAreaUnderPRC() {
        return areaUnderPRC;
    }

    public void setAreaUnderPRC(double areaUnderPRC) {
        this.areaUnderPRC = areaUnderPRC;
    }

    public List<Tuple2<Double, Double>>  getPrecision() {
        return precision;
    }

    public void setPrecision(List<Tuple2<Double, Double>>  precision) {
        this.precision = precision;
    }

    public List<Tuple2<Double, Double>>  getRecall() {
        return recall;
    }

    public void setRecall(List<Tuple2<Double, Double>>  recall) {
        this.recall = recall;
    }

    public List<Tuple2<Double, Double>>  getF1Score() {
        return f1Score;
    }

    public void setF1Score(List<Tuple2<Double, Double>>  f1Score) {
        this.f1Score = f1Score;
    }

    public List<Tuple2<Double, Double>>  getF2Score() {
        return f2Score;
    }

    public void setF2Score(List<Tuple2<Double, Double>>  f2Score) {
        this.f2Score = f2Score;
    }

    public List<Double>  getThresholds() {
        return thresholds;
    }

    public void setThresholds(List<Double> thresholds) {
        this.thresholds = thresholds;
    }

    public List<Tuple2<Double, Double>>  getPrc() {
        return prc;
    }

    public void setPrc(List<Tuple2<Double, Double>>  prc) {
        this.prc = prc;
    }

    public List<Tuple2<Double, Double>>  getRoc() {
        return roc;
    }

    public void setRoc(List<Tuple2<Double, Double>>  roc) {
        this.roc = roc;
    }
   
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Product: " + product).append("\n");
        sb.append("Version: " + version).append("\n");
        sb.append("Name: " + name).append("\n");
        sb.append("Accuracy: " + accuracy * 100).append("\n");
        sb.append("Area under ROC = " + areaUnderROC).append("\n");
        sb.append("Area under precision-recall curve = " + areaUnderPRC).append("\n");
        sb.append("Thresholds: " + thresholds).append("\n");
        sb.append("Precision by threshold: " + precision).append("\n");
        sb.append("Recall by threshold: " + recall).append("\n");
        sb.append("F1 Score by threshold: " + f1Score).append("\n");
        sb.append("F2 Score by threshold: " + f2Score).append("\n");
        sb.append("Precision-recall curve: " + prc).append("\n");
        sb.append("ROC curve: " + roc);
        return sb.toString();
    }
}
