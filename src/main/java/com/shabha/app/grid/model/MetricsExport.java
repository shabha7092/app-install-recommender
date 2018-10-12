package com.shabha.app.grid.model;


public class MetricsExport {

    private String product;
    private String version;
    private String name;
    private double accuracy;
    private double areaUnderROC;
    private double areaUnderPRC;

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

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Product: " + product).append("\n");
        sb.append("Version: " + version).append("\n");
        sb.append("Name: " + name).append("\n");
        sb.append("Accuracy: " + accuracy * 100).append("\n");
        sb.append("Area under ROC = " + areaUnderROC).append("\n");
        sb.append("Area under precision-recall curve = " + areaUnderPRC).append("\n");
        return sb.toString();
    }

}
