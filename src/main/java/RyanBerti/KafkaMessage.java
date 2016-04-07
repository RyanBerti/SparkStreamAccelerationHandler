package RyanBerti;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaMessage implements Serializable
{
    private final String deviceType;
    private final String identifier;
    private final String zone;
    private final String name;
    private final String value;
    private final long timestamp;

    @JsonCreator
    public KafkaMessage(@JsonProperty("deviceType") String deviceType,
            @JsonProperty("identifier") String identifier,
            @JsonProperty("zone") String zone,
            @JsonProperty("name") String name,
            @JsonProperty("value") String value,
            @JsonProperty("timestamp") long timestamp)
    {
        this.deviceType = checkNotNull(deviceType, "deviceType is null");
        this.identifier = checkNotNull(identifier, "identifier is null");
        this.name = checkNotNull(name, "name is null");
        this.value = checkNotNull(value, "value is null");
        this.zone = checkNotNull(zone, "zone is null");
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String getDeviceType()
    {
        return deviceType;
    }

    @JsonProperty
    public String getIdentifier()
    {
        return identifier;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getZone()
    {
        return zone;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @JsonProperty
    public long getTimestamp()
    {
        return timestamp;
    }

    @Override
    public String toString()
    {
        return "KafkaMessage{" +
                "deviceType='" + deviceType + '\'' +
                ", identifier='" + identifier + '\'' +
                ", zone='" + zone + '\'' +
                ", name='" + name + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
