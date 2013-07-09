package org.projectodd.vertx.jmsclient;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.vertx.java.core.Context;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class JMSClientVerticle extends Verticle {

    public static final String DEFAULT_BROKER_ADDRESS = "org.projectodd.jms.broker";
    public static final String DEFAULT_ADDRESS = "org.projectodd.jms.client";

    private Connection connection;
    private Session session;

    @Override
    public void start(final Future<Void> startedResult) {
        System.err.println("============================ CONNECTION STARTING");

        vertx.eventBus().send(DEFAULT_BROKER_ADDRESS, true, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                String creatorClassName = event.body().getString("connection_creator_class");
                try {
                    Class<ConnectionCreator> creatorClass = (Class<ConnectionCreator>) Class.forName(creatorClassName);
                    ConnectionCreator creator = creatorClass.newInstance();
                    JMSClientVerticle.this.connection = creator.create(event.body());
                    JMSClientVerticle.this.session = JMSClientVerticle.this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    JMSClientVerticle.this.connection.start();
                    vertx.eventBus().registerLocalHandler(DEFAULT_ADDRESS, new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> event) {
                            handleEvent(event);
                        }
                    });
                    System.err.println("============================ CONNECTION STARTED");
                    startedResult.setResult(null);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | JMSException e) {
                    startedResult.setFailure(e);
                    return;
                }
            }
        });
    }

    @Override
    public void stop() {
        try {
            System.err.println("============================ CONNECTION STOP");
            this.connection.stop();
            this.connection = null;
            System.err.println("============================ CONNECTION STOPPED");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void handleEvent(final Message<JsonObject> event) {
        if (event.body().getString("subscribe") != null) {
            try {
                String destination = event.body().getString("subscribe");
                System.err.println("SUBSCRIBE: " + destination);
                Session session = this.connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                Queue queue = session.createQueue(destination);
                MessageConsumer consumer = session.createConsumer(queue);
                final Context context = vertx.currentContext();
                consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(final javax.jms.Message message) {
                        context.runOnContext(new Handler<Void>() {
                            @Override
                            public void handle(Void ignored) {
                                System.err.println("** handling onContext: " + message);
                                try {
                                    String address = event.body().getString("address");
                                    System.err.println("sending to: " + address);
                                    vertx.eventBus().send(event.body().getString("address"), ((TextMessage) message).getText(), new Handler<Message<Boolean>>() {
                                        @Override
                                        public void handle(Message<Boolean> event) {
                                            if (event.body()) {
                                                try {
                                                    message.acknowledge();
                                                } catch (JMSException e) {
                                                    e.printStackTrace();
                                                }
                                            }
                                        }
                                    });
                                } catch (JMSException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                });
            } catch (JMSException e) {
                e.printStackTrace();
                event.reply(false);
            }
            event.reply(true);
        } else if (event.body().getString("send") != null) {
            String destination = event.body().getString("send");
            System.err.println("== SEND: " + destination);
            try {
                Session session = this.connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createQueue(destination));
                producer.send(session.createTextMessage(event.body().getString("body")));
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }
}
