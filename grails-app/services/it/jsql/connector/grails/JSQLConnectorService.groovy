package it.jsql.connector.grails

import grails.core.GrailsApplication
import grails.transaction.Transactional
import groovy.json.JsonSlurper
import it.jsql.connector.grails.exception.ErrorMessagesSingleton
import it.jsql.connector.grails.exception.JSQLException
import org.hibernate.exception.SQLGrammarException

@Transactional
class JSQLConnectorService {

    GrailsApplication grailsApplication


    protected def host = "http://softwarecartoon.com:9391/"
    protected def requestQueriesPath = "api/request/queries"
    protected def requestDatabaseOptionsPath = "api/request/options"

    private String apiKey = null
    private String memberKey = null

    def getMemberKey() throws JSQLException {

        memberKey = grailsApplication.config.getProperty('jsql.memberKey')

        if (memberKey == null || memberKey.length() < 1) {
            throw new JSQLException("No memberKey defined")
        }

        return memberKey

    }

    def getApiKey() throws JSQLException {
        apiKey = grailsApplication.config.getProperty('jsql.apiKey')
        if (apiKey == null || apiKey.length() < 1) {
            throw new JSQLException("No apiKey defined")
        }
        return apiKey

    }

    def requestDatabaseOptions() throws JSQLException {
        return this.call("GET", this.requestDatabaseOptionsPath, null)
    }

    def requestQueries(def hashesList, def isArray) throws JSQLException {
        return this.call("POST", this.requestQueriesPath, this.buildJSONRequest(hashesList))

    }

    protected def buildJSONRequest(def hashesList) {

        StringBuilder stringBuilder = new StringBuilder()
        String text = hashesList
        String editedText = ""
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '[' || text.charAt(i) == ']') {
                continue
            }
            editedText += text.charAt(i)
        }
        if (editedText.contains(","))
            return "[" + editedText + "]"

        stringBuilder.append("[")

        for (int i = 0; i < hashesList.size(); i++) {
            stringBuilder.append("\"" + hashesList.get(i) + "\"")
            if (i != hashesList.size() - 1) {
                stringBuilder.append(",")
            }
        }

        stringBuilder.append("]")
        return stringBuilder.toString()
    }

    protected def call(String method, String path, String request) {
        HttpURLConnection conn = null
        String fullUrl = this.host + path
        if (request.contains(",")) {
            fullUrl += "/grouped"
        }
        try {

            URL url = new URL(fullUrl)
            conn = (HttpURLConnection) url.openConnection()
            conn.setDoOutput(true)
            conn.setRequestMethod(method)
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("ApiKey", this.getApiKey())
            conn.setRequestProperty('MemberKey', this.getMemberKey())

            OutputStream os = conn.getOutputStream()
            os.write(request.getBytes())
            os.flush()

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new JSQLException("HTTP error code : " + conn.getResponseCode())
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())))

            StringBuilder builder = new StringBuilder()

            while (br.ready()) {
                builder.append(br.readLine())
            }

            conn.disconnect()

            String responseJSON = builder.toString()

            if (!responseJSON.isEmpty()) {
                return new JsonSlurper().parseText(responseJSON)
            } else {
                return []
            }

        } catch (MalformedURLException e) {
            e.printStackTrace()
            ErrorMessagesSingleton.getInstance().setMessage(e.getMessage())
        } catch (IOException e) {
            e.printStackTrace()
            ErrorMessagesSingleton.getInstance().setMessage(e.getMessage())
        } catch (JSQLException e) {
            e.println(e.getMessage())
            e.println(e.getCause())
        } catch (SQLGrammarException e) {
            e.println(e.getMessage())
            e.println(e.getCause())
        }

        return []

    }

}
