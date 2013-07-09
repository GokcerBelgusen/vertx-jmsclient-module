package org.projectodd.vertx.jmsclient;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.vertx.java.core.json.JsonObject;

public interface ConnectionCreator {

    Connection create(JsonObject body) throws JMSException;

}
