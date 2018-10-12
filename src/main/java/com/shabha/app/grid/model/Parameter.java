package com.shabha.app.grid.model;

public enum Parameter {
    
    INPUT_PATH("input_path"),
    CONFIG_PATH("config_path"),
    OUTPUT_PATH("output_path"),
    TEST_FLAG("test_flag");
    
    private String value;

    private Parameter(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }   
    
}
