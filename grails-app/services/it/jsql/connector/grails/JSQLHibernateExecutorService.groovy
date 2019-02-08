package it.jsql.connector.grails

import grails.transaction.Transactional
import org.hibernate.Session
import org.hibernate.SessionFactory
import org.hibernate.jdbc.Work
import org.springframework.beans.factory.annotation.Autowired

import java.sql.*

@Transactional
class JSQLHibernateExecutorService {

    SessionFactory sessionFactory

    @Autowired
    JSQLService jsqlService

    void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory
    }

    def executeSelect(String sql, Map<String, Object> params) {
        List<Map<String, Object>> list = new ArrayList<>()
        Session session = sessionFactory.getCurrentSession()
        Map<String, Object> map = new HashMap<>()
        try {
            session.doWork(new Work() {
                @Override
                void execute(Connection connection) throws SQLException {
                    Statement st = connection.createStatement()
                    String finalSql = sql
                    ArrayList<String> parameters = parameters(finalSql)

                    for (Map.Entry<String, Object> m : params.entrySet()) {
                        if (!(m.value == null))
                            parameters.remove(":" + m.key)
                        if (m.getValue() instanceof String) {
                            finalSql = finalSql.replace(":" + m.getKey(), "'" + m.getValue() + "'")
                        } else {
                            finalSql = finalSql.replace(":" + m.getKey(), String.valueOf(m.getValue()))
                        }
                    }

                    if (parameters.size() > 0)
                        throw new SQLException("There are missing parameters: " + parameters)

                    println "finalSql = " + finalSql
                    ResultSet rs = st.executeQuery(finalSql)
                    ResultSetMetaData resultSetMetaData = rs.getMetaData()
                    while (rs.next()) {
                        map = new HashMap<>()
                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                            map.put(JSQLUtils.toCamelCase(resultSetMetaData.getColumnName(i)), rs.getObject(i))
                        }

                        list.add(map)
                    }
                }


            })
        } catch (Exception e) {
            getExceptionCause(list, map, e)
            e.printStackTrace()
        }
        return list
    }

    @Transactional
    int executeDelete(String sql, Map<String, Object> params) throws SQLException {
        executeSQLNoResult(sql, params, "delete")
    }


    @Transactional
    void executeUpdate(String sql, Map<String, Object> params) throws SQLException {
        executeSQLNoResult(sql, params)
    }

    @Transactional
    BigInteger executeInsert(String sql, Map<String, Object> params) throws Exception {
        Session session = sessionFactory.getCurrentSession()
        jsqlService.assignDatabaseDialect()
        if (JSQLService.DATABASE_DIALECT == "POSTGRES")
            sql = JSQLUtils.buildReturningId(sql)
        Long lastId = 1


        String finalSql = sql
        session.doWork(new Work() {
            @Override
            void execute(Connection connection) throws Exception {
                Statement st = connection.createStatement()

                ArrayList<String> parameters = parameters(finalSql)
                for (Map.Entry<String, Object> m : params.entrySet()) {
                    if (!(m.value == null))
                        parameters.remove(":" + m.key)
                    if (m.getValue() instanceof String) {
                        finalSql = finalSql.replace(":" + m.getKey(), "'" + m.getValue() + "'")
                    } else {
                        finalSql = finalSql.replace(":" + m.getKey(), String.valueOf(m.getValue()))
                    }
                }

                if (parameters.size() > 0)
                    throw new SQLException("There are missing parameters: " + parameters)

                ResultSet rs
                if (JSQLService.DATABASE_DIALECT == "POSTGRES") {
                    rs = st.executeQuery(finalSql)
                    while (rs.next()) {
                        lastId = rs.getLong(1)
                    }
                } else {
                    st.executeUpdate(finalSql)
                    rs = st.executeQuery("SELECT LAST_INSERT_ID()")
                    while (rs.next()) {
                        lastId = rs.getLong(1)
                    }
                }
            }
        })

        return BigInteger.valueOf(lastId)
    }

    private void executeSQLNoResult(sql, params) throws SQLException {
        Session session = sessionFactory.getCurrentSession()

        session.doWork(new Work() {

            @Override
            void execute(Connection connection) throws SQLException {
                Statement st = connection.createStatement()
                String finalSql = sql
                ArrayList<String> parameters = parameters(finalSql)
                for (Map.Entry<String, Object> m : params.entrySet()) {
                    if (!(m.value == null))
                        parameters.remove(":" + m.key)
                    if (m.getValue() instanceof String) {
                        finalSql = finalSql.replace(":" + m.getKey(), "'" + m.getValue() + "'")
                    } else {
                        finalSql = finalSql.replace(":" + m.getKey(), String.valueOf(m.getValue()))
                    }
                }

                if (parameters.size() > 0)
                    throw new SQLException("There are missing parameters: " + parameters)
                st.executeUpdate(finalSql)

            }
        })

    }

    private int executeSQLNoResult(sql, params, delete) throws SQLException {
        Session session = sessionFactory.getCurrentSession()
        int executed = 0
        session.doWork(new Work() {

            @Override
            void execute(Connection connection) throws SQLException {
                Statement st = connection.createStatement()
                String finalSql = sql
                ArrayList<String> parameters = parameters(finalSql)
                for (Map.Entry<String, Object> m : params.entrySet()) {
                    if (!(m.value == null))
                        parameters.remove(":" + m.key)
                    if (m.getValue() instanceof String) {
                        finalSql = finalSql.replace(":" + m.getKey(), "'" + m.getValue() + "'")
                    } else {
                        finalSql = finalSql.replace(":" + m.getKey(), String.valueOf(m.getValue()))
                    }
                }

                if (parameters.size() > 0)
                    throw new SQLException("There are missing parameters: " + parameters)
                executed = st.executeUpdate(finalSql)
            }
        })

        return executed

    }

    private void getExceptionCause(List<Map<String, Object>> list, Map<String, Object> response, Exception e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        response.put("code", 400);
        response.put("description", cause.getMessage().split("\n")[0]);
        list.add(response);
    }

    ArrayList<String> parameters(String finalSql) {
        String[] sqls = finalSql.split("[ ,()=]")
        List<String> parameters = new ArrayList<>()
        for (String s : sqls) {
            if (s.startsWith(":"))
                parameters.add(s)
        }
        parameters
    }
}
