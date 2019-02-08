package it.jsql.connector.grails

import grails.transaction.Transactional
import it.jsql.connector.grails.exception.JSQLException
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.hibernate.exception.SQLGrammarException
import org.springframework.beans.factory.annotation.Autowired

import java.sql.SQLException

@Transactional
class JSQLService {
    public static String DATABASE_DIALECT = null
    final def LAST_ID_NAME = "lastId"
    final def PARAMS_NAME = "params"
    final def HASH_NAME = "token"

    @Autowired
    JSQLConnectorService jsqlConnectorService

    @Autowired
    JSQLHibernateExecutorService jsqlHibernateExecutorService

    void setJsqlConnectorService(JSQLConnectorService jsqlConnectorService) {
        this.jsqlConnectorService = jsqlConnectorService
    }

    void setJsqlHibernateExecutorService(JSQLHibernateExecutorService jsqlHibernateExecutorService) {
        this.jsqlHibernateExecutorService = jsqlHibernateExecutorService
    }

    def select(def data) {
        String sql
        Logger logger = Logger.getLogger("org.hibernate.engine.jdbc.spi.SqlExceptionHelper")
        logger.setLevel(Level.ALL)

        try {
            sql = this.getSQLQuery(data)
            if (sql.contains("code"))
                return this.getSQLQuery(data)
        } catch (NullPointerException e) {
            if (sql == null) {
                return [code: 400, message: "No such hash for given ApiKey and MemberKey"]
            }
            return [code: 400, message: e.getMessage()]
        } catch (Exception e) {
            return [code: 400, message: e.getMessage()]
        }

        sql = sql.trim()
        String[] words = sql.split("\\s+")
        if (!words[0].equalsIgnoreCase("select"))
            return [code: 400, message: "Only SELECT queries allowed"]

        def paramsMap = this.getParamsMap(data)
        ArrayList ex
        try {
            ex = jsqlHibernateExecutorService.executeSelect(sql, paramsMap)
        } catch (Exception e) {
            return [code: 400, message: e.getMessage()]
        }
        return ex
    }

    def delete(def data) {
        String sql
        try {
            sql = this.getSQLQuery(data)
            if (sql.contains("code"))
                return this.getSQLQuery(data)
        } catch (Exception e) {
            return [code: 400, message: e.getMessage()]
        }

        sql = sql.trim()
        String[] words = sql.split("\\s+")
        if (!words[0].equalsIgnoreCase("delete"))
            return [code: 400, message: "Only DELETE queries allowed"]

        def paramsMap = this.getParamsMap(data)
        try {
            int executed = jsqlHibernateExecutorService.executeDelete(sql, paramsMap)
            if (executed == 0)
                return [code: 400, message: "given column does not exist"]
        } catch (SQLException e) {
            return [code: 400, message: e.getCause().localizedMessage]
        } catch (SQLGrammarException e) {
            return [code: 400, message: e.getCause().localizedMessage]
        }
        return [status: "OK"]
    }

    def update(def data) {
        String sql
        try {
            sql = this.getSQLQuery(data)
            if (sql.contains("code"))
                return this.getSQLQuery(data)
        } catch (Exception e) {
            return [code: 400, message: e.getMessage()]
        }
        sql = sql.trim()
        String[] words = sql.split("\\s+")
        if (!words[0].equalsIgnoreCase("update"))
            return [code: 400, message: "Only UPDATE queries allowed"]

        def paramsMap = this.getParamsMap(data)
        ArrayList ex
        try {
            jsqlHibernateExecutorService.executeUpdate(sql, paramsMap)
        } catch (Exception e) {
            return [code: 400, message: e.getCause().message]
        }

        return [status: "OK"]
    }

    def insert(def data) {
        String sql
        try {
            sql = this.getSQLQuery(data)
            if (sql.contains("code"))
                return this.getSQLQuery(data)
        } catch (Exception e) {
            return [code: 400, message: e.getMessage()]
        }

        sql = sql.trim()
        String[] words = sql.split("\\s+")
        if (!words[0].equalsIgnoreCase("insert"))
            return [code: 400, message: "Only INSERT queries allowed"]

        def paramsMap = this.getParamsMap(data)
        BigInteger lastId
        try {
            lastId = jsqlHibernateExecutorService.executeInsert(sql, paramsMap)
        } catch (Exception e) {
            return [code: 400, message: e.getCause().message]
        }
        def response = [:]
        response["code"] = 200
        response[LAST_ID_NAME] = lastId

        return response
    }

    def getHash(def data) {
        return data[HASH_NAME]
    }

    def getParamsMap(Map<String, Object> data) {

        Object params = data[PARAMS_NAME]

        if (params instanceof Map) {
            return (Map<String, Object>) params
        }

        return new HashMap<>()
    }


    def getDBOptions() {

        try {

            def options = jsqlConnectorService.requestDatabaseOptions()

            return options

        } catch (JSQLException e) {
            e.printStackTrace()
        }

        return null


    }

    def getSQLQuery(def data) {

        try {

            def queries

            if (data instanceof List) {

                def listOfHashes = []

                data.toArray().each {
                    listOfHashes.push(this.getHash(it))
                }

                queries = jsqlConnectorService.requestQueries(listOfHashes, true)
            } else {
                queries = jsqlConnectorService.requestQueries([this.getHash(data)], false)
                if (queries.toString().contains("code"))
                    return queries
            }

            return queries[0].query

        } catch (JSQLException e) {
            e.printStackTrace()
        }

        return null

    }

    void assignDatabaseDialect() {
        if (DATABASE_DIALECT == null) {
            try {
                this.getDatabaseDialect()
            } catch (Exception e) {
                e.printStackTrace()
            }
        }
    }

    private getDatabaseDialect() throws Exception {

        StringBuilder result = new StringBuilder()
        URL url = new URL("http://softwarecartoon.com:9391/api/request/options/all")
        HttpURLConnection conn = (HttpURLConnection) url.openConnection()
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setRequestProperty("ApiKey", this.jsqlConnectorService.getApiKey())
        conn.setRequestProperty("MemberKey", this.jsqlConnectorService.getMemberKey())
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()))
        String line
        while ((line = rd.readLine()) != null) {
            result.append(line)
        }
        rd.close()

        String[] words = result.toString().split(",")
        for (int i = 0; i < words.length; i++) {
            if (words[i].contains("databaseDialect")) {
                String[] dialect = words[i].split(":")
                DATABASE_DIALECT = dialect[1].substring(1, dialect[1].length() - 1)
            }
        }

    }

    def printError(def err) {
        return [code: 400, message: err]
    }
}
