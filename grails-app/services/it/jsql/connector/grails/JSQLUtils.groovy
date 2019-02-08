package it.jsql.connector.grails

class JSQLUtils {

    static String toCamelCase(String str) {
        String[] parts = str.split("_")
        String camelCaseString = ""
        for (int i = 0; i < parts.length; i++) {
            if (i != 0) {
                parts[i] = parts[i].substring(0, 1).toUpperCase() + parts[i].substring(1).toLowerCase()
            }
            camelCaseString += parts[i]
        }
        return camelCaseString
    }

    static String buildReturningId(String sql) {
        if (!sql.toLowerCase().contains("returning")) {
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1)
            }
            sql += " RETURNING id"
        }
        return sql
    }

}
