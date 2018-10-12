package com.shabha.app.grid.model;

import java.util.Arrays;

public class ModelProperties {
  
    private String product;
    private String version;
    private String name;    
    private String[] features;
    private String label;
    private ModelConfiguration configuration;
    
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
    
    public String[] getFeatures() {
        return features;
    }
    
    public void setFeatures(String[] features) {
        this.features = features;
    }
    
    public String getLabel() {
        return label;
    }
    
    public void setLabel(String label) {
        this.label = label;
    }
    
    public ModelConfiguration getConfiguration() {
        return configuration;
    }
    
    public void setConfiguration(ModelConfiguration configuration) {
        this.configuration = configuration;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("product:" + product).append("\n");
        sb.append("version:" + version).append("\n");
        sb.append("name:" + name).append("\n");
        sb.append("features:" + (features == null ? "" : Arrays.toString(features))).append("\n");
        sb.append("label:" + label).append("\n");
        sb.append("configuration:" + configuration.toString());
        return sb.toString();
    }
}
