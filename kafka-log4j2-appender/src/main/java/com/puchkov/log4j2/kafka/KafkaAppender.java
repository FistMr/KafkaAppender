package com.puchkov.log4j2.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Properties;

@Plugin(name = "MyKafka", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class KafkaAppender extends AbstractAppender {

    private final Producer<String, String> producer;
    private final String topic;

    protected KafkaAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                            boolean ignoreExceptions, Property[] properties, String topic) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.topic = topic;
        this.producer = createProducer(properties);
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Properties") Property[] properties) {

        if (name == null) {
            LOGGER.error("No name provided for KafkaAppender");
            return null;
        }

        if (topic == null) {
            LOGGER.error("No topic provided for KafkaAppender");
            return null;
        }

        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        return new KafkaAppender(name, filter, layout, ignoreExceptions, properties, topic);
    }


    @Override
    public void append(LogEvent event) {
        String message = new String(getLayout().toByteArray(event));
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
        producer.send(producerRecord);

    }

    private Producer<String, String> createProducer(Property[] properties) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.RETRIES_CONFIG, "3");

        if (properties != null) {
            for (Property property : properties) {
                config.put(property.getName(), property.getValue());
            }
        }

        return new KafkaProducer<>(config);
    }
}
