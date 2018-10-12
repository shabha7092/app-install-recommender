package com.shabha.app.grid.model;

import java.util.Arrays;

public class ModelConfiguration {
        
    private int[] maxIterations;
    private double[] regParameters;
    private double[] elasticNetParameters;
    private boolean fitIntercept;
    private int numsFolds;
    
    
    public int[] getIterations() {
        return maxIterations;
    }
    
    public void setIterations(int[] maxIterations) {
        this.maxIterations = maxIterations;
    }
    
    public double[] getParameters() {
        return regParameters;
    }
    
    public void setParameters(double[] regParameters) {
        this.regParameters = regParameters;
    }
    
    public double[] getElasticParameters() {
        return elasticNetParameters;
    }
    
    public void setElasticParameters(double[] elasticNetParameters) {
        this.elasticNetParameters = elasticNetParameters;
    }
    
    public boolean isFit() {
        return fitIntercept;
    }
    
    public void setFit(boolean fitIntercept) {
        this.fitIntercept = fitIntercept;
    }
    
    public int getFolds() {
        return numsFolds;
    }
    
    public void setFolds(int numsFolds) {
        this.numsFolds = numsFolds;
    }
    
   public String toString() {
       StringBuilder sb = new StringBuilder();
       sb.append("maxIterations:" + (maxIterations == null ? "" : Arrays.toString(maxIterations))).append("\n");
       sb.append("regParameters:" + (regParameters == null ? "" : Arrays.toString(regParameters))).append("\n");
       sb.append("elasticNetParameters:" + (elasticNetParameters == null ? "" : Arrays.toString(elasticNetParameters))).append("\n");
       sb.append("fitIntercept:" + fitIntercept).append("\n");
       sb.append("numsFolds:" + numsFolds);
       return sb.toString();
   }
}
