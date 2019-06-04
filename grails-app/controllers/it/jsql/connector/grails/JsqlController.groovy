package it.jsql.connector.grails

import grails.converters.JSON
import grails.core.GrailsApplication
import it.jsql.connector.grails.dto.JSQLConfig
import it.jsql.connector.grails.dto.JSQLResponse
import it.jsql.connector.grails.jsql.JSQLConnector
import org.grails.web.json.JSONObject

class JsqlController {

    static responseFormats = ['json']

    static allowedMethods = [
            select  : 'POST',
            delete  : 'POST',
            update  : 'POST',
            insert  : 'POST',
            rollback: 'POST',
            commit  : 'POST'
    ]

    GrailsApplication grailsApplication

    public static final String TRANSACTION_ID = "txid";

    private static final String API_URL = "https://provider.jsql.it/api/jsql";

    private String getApiKey() {
        return grailsApplication.config.getProperty('jsql.apiKey')
    }

    private String getDevKey() {
        return grailsApplication.config.getProperty('jsql.devKey')
    }

    private String getProviderUrl() {
        String providerUrl = grailsApplication.config.getProperty('jsql.providerUrl');

        if(!providerUrl){
            return API_URL;
        }

        return providerUrl;

    }

    private JSQLConfig jsqlConfig = null;

    private JSQLConfig getConfig() {

        if (this.jsqlConfig == null) {
            this.jsqlConfig = new JSQLConfig(this.getApiKey(), this.getDevKey());
        }

        return this.jsqlConfig;
    }

    def index() {
        def status = [plugin: 'JSQL', status: "OK"]
        render status as JSON
    }

    def select() {

        JSONObject data = request.JSON
        String transactionId = request.getHeader(TRANSACTION_ID)

        JSQLResponse jsqlResponse = JSQLConnector.callSelect(transactionId, this.getProviderUrl(), data, this.getConfig());

        if (jsqlResponse.transactionId != null) {
            response.setHeader(TRANSACTION_ID, jsqlResponse.transactionId);
        }

        render jsqlResponse.response as JSON

    }

    def delete() {

        JSONObject data = request.JSON
        String transactionId = request.getHeader(TRANSACTION_ID)

        JSQLResponse jsqlResponse = JSQLConnector.callDelete(transactionId, this.getProviderUrl(), data, this.getConfig());

        if (jsqlResponse.transactionId != null) {
            response.setHeader(TRANSACTION_ID, jsqlResponse.transactionId);
        }

        render jsqlResponse.response as JSON

    }

    def update() {

        JSONObject data = request.JSON
        String transactionId = request.getHeader(TRANSACTION_ID)

        JSQLResponse jsqlResponse = JSQLConnector.callUpdate(transactionId, this.getProviderUrl(), data, this.getConfig());

        if (jsqlResponse.transactionId != null) {
            response.setHeader(TRANSACTION_ID, jsqlResponse.transactionId);
        }

        render jsqlResponse.response as JSON

    }

    def insert() {

        JSONObject data = request.JSON
        String transactionId = request.getHeader(TRANSACTION_ID)

        JSQLResponse jsqlResponse = JSQLConnector.callInsert(transactionId, this.getProviderUrl(), data, this.getConfig());

        if (jsqlResponse.transactionId != null) {
            response.setHeader(TRANSACTION_ID, jsqlResponse.transactionId);
        }

        render jsqlResponse.response as JSON

    }

    def rollback() {

        String transactionId = request.getHeader(TRANSACTION_ID)

        JSQLResponse jsqlResponse = JSQLConnector.callRollback(this.getProviderUrl(), transactionId, this.getConfig());

        render jsqlResponse.response as JSON

    }

    def commit() {

        String transactionId = request.getHeader(TRANSACTION_ID)

        JSQLResponse jsqlResponse = JSQLConnector.callCommit(this.getProviderUrl(), transactionId, this.getConfig());

        render jsqlResponse.response as JSON

    }

}
