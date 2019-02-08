package it.jsql.connector.grails.exception

/**
 * Created by Dawid on 2016-09-13.
 */
class JSQLException  extends Exception {

    public JSQLException(){
        super();
    }

    public JSQLException(String message){
        super("jSQL: "+message);
    }
}
