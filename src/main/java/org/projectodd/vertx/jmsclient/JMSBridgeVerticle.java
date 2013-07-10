package org.projectodd.vertx.jmsclient;

import javax.jms.Connection;
import javax.jms.Destination;
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
import org.vertx.java.core.VertxException;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class JMSBridgeVerticle extends Verticle {

    public static final String DEFAULT_BROKER_ADDRESS = "org.projectodd.jms.broker";
    public static final String DEFAULT_ADDRESS = "org.projectodd.jms.bridge";

    private Connection connection;
    private Session session;

    @Override
    public void start(final Future<Void> startedResult) {
        System.err.println( "starting bridge" );
        String queueName = container.config().getString("queue");
        String topicName = container.config().getString("topic");

        String jmsDestination = null;
        boolean isTopic = false;

        if (queueName == null && topicName == null) {
            startedResult.setFailure(new VertxException("Either queue or topic must be specified"));
            return;
        } else if (queueName != null && topicName != null) {
            startedResult.setFailure(new VertxException("Only queue or topic may be specified, not both"));
            return;
        } else if (queueName != null) {
            jmsDestination = queueName;
        } else if (topicName != null) {
            jmsDestination = topicName;
            isTopic = true;
        }
        
        String inboundAddress = container.config().getString("inbound_address");
        String outboundAddress = container.config().getString("outbound_address");
        
        if ( inboundAddress == null && outboundAddress == null ) {
            startedResult.setFailure( new VertxException( "At least one of inbound_address or outbound_address must be specified" ) );
            return;
        }

        JsonObject creator = container.config().getObject("creator");
        if (creator != null) {
            String creatorClassName = creator.getString("class");
            JsonObject creatorConfig = creator.getObject("config");
            setup(creatorClassName, creatorConfig, jmsDestination, isTopic, inboundAddress, outboundAddress, startedResult);
        } else {
            String brokerAddress = container.config().getString("broker_address");
            if (brokerAddress == null) {
                brokerAddress = DEFAULT_BROKER_ADDRESS;
            }
            setup(brokerAddress, jmsDestination, isTopic, inboundAddress, outboundAddress, startedResult);
        }
    }

    protected void setup(
            final String creatorClassName,
            final JsonObject creatorConfig,
            final String jmsDestination,
            final boolean isTopic,
            final String inboundAddress,
            final String outboundAddress,
            final Future<Void> startedResult) {
        try {
            setupJmsConnection(creatorClassName, creatorConfig);
            setupFlow(jmsDestination, isTopic, inboundAddress, outboundAddress);
            startedResult.setResult(null);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | JMSException e) {
            startedResult.setFailure(e);
            return;
        }
    }

    protected void setup(
            final String brokerAddress,
            final String jmsDestination,
            final boolean isTopic,
            final String inboundAddress,
            final String outboundAddress,
            final Future<Void> startedResult) {
        vertx.eventBus().send(brokerAddress, true, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                String creatorClassName = event.body().getString("connection_creator_class");
                try {
                    setupJmsConnection(creatorClassName, event.body());
                    setupFlow(jmsDestination, isTopic, inboundAddress, outboundAddress);
                    startedResult.setResult(null);
                } catch (JMSException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    startedResult.setFailure(e);
                }
            }
        });
    }

    protected void setupJmsConnection(String creatorClassName, JsonObject config) throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            JMSException {
        Class<ConnectionCreator> creatorClass = (Class<ConnectionCreator>) Class.forName(creatorClassName);
        ConnectionCreator creator = creatorClass.newInstance();
        JMSBridgeVerticle.this.connection = creator.create(config);
        JMSBridgeVerticle.this.session = JMSBridgeVerticle.this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        JMSBridgeVerticle.this.connection.start();
    }

    protected void setupFlow(
            final String jmsDestination,
            final boolean isTopic,
            final String inboundAddress,
            final String outboundAddress) throws JMSException {
        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = null;
        if (isTopic) {
            destination = this.session.createTopic(jmsDestination);
        } else {
            destination = this.session.createQueue(jmsDestination);
        }

        if (inboundAddress != null) {
            MessageConsumer consumer = this.session.createConsumer( destination );
            consumer.setMessageListener(new VertxMessageListener(vertx, vertx.currentContext(), inboundAddress));
        }

        if (outboundAddress != null) {
            vertx.eventBus().registerHandler(outboundAddress, new VertxHandler(this.session, this.session.createProducer(destination)));
        }
    }

    @Override
    public void stop() {
        try {
            this.connection.stop();
            this.connection = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
