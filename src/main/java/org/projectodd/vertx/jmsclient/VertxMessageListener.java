package org.projectodd.vertx.jmsclient;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;

public class VertxMessageListener implements MessageListener {

    private Vertx vertx;
    private Context context;
    private String address;

    public VertxMessageListener(Vertx vertx, Context context, String address) {
        this.vertx = vertx;
        this.context = context;
        this.address = address;
    }

    @Override
    public void onMessage(Message message) {
        this.context.runOnContext(new MessageHandler(message));
    }

    protected Object convert(Message jmsMessage) throws JMSException {
        if (jmsMessage instanceof TextMessage) {
            String body = ((TextMessage) jmsMessage).getText();
            String contentType = jmsMessage.getStringProperty("Content-Type");
            if (contentType != null) {
                if ( contentType.equals( "application/json" ) ) {
                    return new JsonObject( body );
                }
            }
            return body;
        } else if (jmsMessage instanceof BytesMessage) {
            byte[] buf = new byte[(int) ((BytesMessage) jmsMessage).getBodyLength()];
            return new Buffer(((BytesMessage) jmsMessage).readBytes(buf));
        }
        return null;
    }

    private final class MessageHandler implements Handler<Void> {

        private Message jmsMessage;

        MessageHandler(Message message) {
            this.jmsMessage = message;
        }

        @Override
        public void handle(Void event) {
            try {
                vertx.eventBus().send(address, convert(jmsMessage));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
