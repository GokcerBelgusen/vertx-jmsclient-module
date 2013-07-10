package org.projectodd.vertx.jmsclient;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class VertxHandler implements Handler<Message<?>> {

    private Session session;
    private MessageProducer producer;

    public VertxHandler(Session session, MessageProducer producer) {
        this.session = session;
        this.producer = producer;
    }

    @Override
    public void handle(Message<?> event) {
        Object body = event.body();
        javax.jms.Message jmsMessage = null;
        String contentType = null;
        if (body instanceof JsonObject ) {
            try {
                jmsMessage = session.createTextMessage(body.toString());
                contentType = "application/json";
            } catch (JMSException e) {
                e.printStackTrace();
            }
        } else if ( body instanceof String ) {
            try {
                jmsMessage = session.createTextMessage(body.toString());
                contentType = "text/plain";
            } catch (JMSException e) {
                e.printStackTrace();
            }
        } else if (body instanceof Buffer) {
            try {
                jmsMessage = session.createBytesMessage();
                contentType = "application/octet-stream";
                ((BytesMessage) jmsMessage).writeBytes(((Buffer) body).getBytes());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        if (jmsMessage != null) {
            try {
                jmsMessage.setStringProperty("Content-Type", contentType );
                this.producer.send(jmsMessage);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

}
