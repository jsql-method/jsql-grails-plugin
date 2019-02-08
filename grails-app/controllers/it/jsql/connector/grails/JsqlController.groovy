package it.jsql.connector.grails

import grails.converters.JSON
import org.grails.web.json.JSONException
import org.springframework.beans.factory.annotation.Autowired

class JsqlController {

    static allowedMethods = [select: 'POST', delete: 'POST', update: 'POST', insert: 'POST']

    @Autowired
    JSQLService jsqlService

    void setJsqlService(JSQLService jsqlService) {
        this.jsqlService = jsqlService
    }

    def index() {
    }

    def select() {
        try {
            render jsqlService.select(request.JSON) as JSON
        } catch (Exception e) {
            render jsqlService.printError(e.getCause().message) as JSON
        }
    }

    def delete() {
        try {
            render jsqlService.delete(request.JSON) as JSON
        } catch (Exception e) {
            render jsqlService.printError(e.getCause().message) as JSON
        }

    }

    def update() {
        try {
            render jsqlService.update(request.JSON) as JSON
        } catch (Exception e) {
            render jsqlService.printError(e.getCause().message) as JSON
        }

    }

    def insert() {
        try {
            render jsqlService.insert(request.JSON) as JSON
        } catch (Exception e) {
            render jsqlService.printError(e.getCause().message) as JSON
        }
    }


}
