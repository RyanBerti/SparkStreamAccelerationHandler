package RyanBerti;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaMessageProducer
{
    private final String brokers;
    private final String kafkaTopic;

    public KafkaMessageProducer(String brokers, String kafkaTopic)
    {
        this.brokers = checkNotNull(brokers, "brokers is null");
        this.kafkaTopic = checkNotNull(kafkaTopic, "kafkaTopic is null");
    }

    public void sendMessage(KafkaMessage message)
    {

        ObjectMapper objectMapper = new ObjectMapper();
        Producer<String, String> producer = createProducer();
        try {
            String identifier = message.getIdentifier();
            String serializedMessage = objectMapper.writeValueAsString(message);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(kafkaTopic, identifier, serializedMessage);
            producer.send(data);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
        finally {
            producer.close();
        }
    }

    private Producer<String, String> createProducer()
    {
        return new Producer<String, String>(createProducerConfig());
    }

    private ProducerConfig createProducerConfig()
    {
        return new ProducerConfig(createProducerProperties());
    }

    private Properties createProducerProperties()
    {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", brokers);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        return properties;
    }
}
