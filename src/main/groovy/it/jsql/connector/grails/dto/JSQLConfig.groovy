package it.jsql.connector.grails.dto

public class JSQLConfig {

    private String apiKey;
    private String devKey;
    private Integer readTimeout = 10000;
    private Integer connectTimeout = 15000;

    public JSQLConfig(String apiKey, String devKey) {
        this.apiKey = apiKey;
        this.devKey = devKey;
    }

    public JSQLConfig(String apiKey, String devKey, Integer connectTimeout) {
        this.apiKey = apiKey;
        this.devKey = devKey;
        this.connectTimeout = connectTimeout;
    }

    public JSQLConfig(String apiKey, String devKey, Integer connectTimeout, Integer readTimeout) {
        this.apiKey = apiKey;
        this.devKey = devKey;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getDevKey() {
        return devKey;
    }

    public void setDevKey(String devKey) {
        this.devKey = devKey;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Integer readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }
}
