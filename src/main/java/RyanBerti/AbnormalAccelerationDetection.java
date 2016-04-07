package RyanBerti;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AbnormalAccelerationDetection
{

    private static final String RESPONSE_MESSAGE_NAME = "color";
    private static final String UNDER_THRESHOLD_VALUE = "green";
    private static final String BEYOND_THRESHOLD_VALUE = "red";

    private final String zone;
    private final List<KafkaMessage> messages;
    private final KafkaMessageProducer kafkaMessageProducer;
    private final double threshold;

    public AbnormalAccelerationDetection(String zone, Iterable<KafkaMessage> messages, KafkaMessageProducer kafkaMessageProducer, double threshold)
    {
        this.zone = checkNotNull(zone, "zone is null");
        this.messages = ImmutableList.copyOf(messages);
        this.kafkaMessageProducer = checkNotNull(kafkaMessageProducer, "kafkaMessageProducer is null");
        this.threshold = threshold;
    }

    public void sendEventOnAbnormalAcceleration()
    {
        System.out.println("APPLICATION OUTPUT\n\n");
        System.out.println("Event count for zone " + zone + ": " + messages.size());
        System.out.println("______________________");

        List<Double> accelerationList = new ArrayList<Double>();
        for (KafkaMessage message : messages) {
            checkState(message.getName().equals("accelerationXYZ"), "unexpected message detected %s", message);
            Acceleration acceleration = Acceleration.valueOf(message.getValue());
            accelerationList.add(acceleration.getAbsAcceleration());
        }
        if (accelerationList.size() == 0) {
            return;
        }

        Collections.sort(accelerationList);
        double medianAcceleration = accelerationList.get(accelerationList.size() / 2);
        System.out.println("Median acceleration value: " + medianAcceleration);

        if (medianAcceleration > threshold) {
            sendThresholdReachedNotificationToAllDevices(BEYOND_THRESHOLD_VALUE);
        }
        else {
            sendThresholdReachedNotificationToAllDevices(UNDER_THRESHOLD_VALUE);
        }
    }

    private void sendThresholdReachedNotificationToAllDevices(String value)
    {
        long now = System.currentTimeMillis();
        Map<String, KafkaMessage> responseMessagesMap = new HashMap<String, KafkaMessage>();
        for (KafkaMessage incomingMessage : messages) {
            KafkaMessage outgoingMessage = new KafkaMessage(
                    incomingMessage.getDeviceType(),
                    incomingMessage.getIdentifier(),
                    incomingMessage.getZone(),
                    RESPONSE_MESSAGE_NAME,
                    value,
                    now
            );
            responseMessagesMap.put(incomingMessage.getIdentifier(), outgoingMessage);
        }

        for (KafkaMessage outgoingMessage : responseMessagesMap.values()) {
            kafkaMessageProducer.sendMessage(outgoingMessage);
        }
    }
}
