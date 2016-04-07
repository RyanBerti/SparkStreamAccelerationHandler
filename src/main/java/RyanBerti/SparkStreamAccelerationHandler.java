package RyanBerti;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class SparkStreamAccelerationHandler
{

    private static final Duration BATCH_DURATION = Durations.seconds(1);
    private static final Duration WINDOW_DURATION = Durations.seconds(5);
    private static final Duration SLIDE_DURATION = Durations.seconds(1);

    public static void main(String[] args)
    {
        if (args.length < 3) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        final String brokers = args[0];
        final String incomingEventsTopic = args[1];
        final String outgoingEventsTopic = args[2];
        final double threshold = Double.valueOf(args[3]);

        /**
         * Setup the spark streaming context with the given batch duration
         */
        SparkConf sparkConf = new SparkConf().setAppName("AbnormalAccelerationDetection");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, BATCH_DURATION);

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(incomingEventsTopic.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        /**
         * Create input stream from kafka brokers and topics; define decoders
         * for pair's key and values
          */
        JavaPairInputDStream<String, KafkaMessage> deviceIdToMessagePairStream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                KafkaMessage.class,
                StringDecoder.class,
                KafkaMessageDecoder.class,
                kafkaParams,
                topicsSet
        );

        /**
         * Remove messages from stream that aren't accelerationXYZ messages
         */
        JavaPairDStream<String, KafkaMessage> filteredStream = deviceIdToMessagePairStream.filter(new Function<Tuple2<String, KafkaMessage>, Boolean>()
        {
            public Boolean call(Tuple2<String, KafkaMessage> stringKafkaMessageTuple2)
                    throws Exception
            {
                return stringKafkaMessageTuple2._2().getName().equals("accelerationXYZ");
            }
        });

        /**
         * Take each incoming message and generate a new pair where key is message zone
         * and value is the message itself
         */
        JavaPairDStream<String, KafkaMessage> zoneToMessagePairStream = filteredStream.mapToPair(
                new PairFunction<Tuple2<String, KafkaMessage>, String, KafkaMessage>()
                {
                    public Tuple2<String, KafkaMessage> call(Tuple2<String, KafkaMessage> deviceIdAndMessage)
                            throws Exception
                    {
                        KafkaMessage message = deviceIdAndMessage._2();
                        return new Tuple2<String, KafkaMessage>(message.getZone(), message);
                    }
                });

        /**
         * Window the stream - this allows us to access a subset of the stream's underlying
         * RDDs, where the subset is defined by the window and slide durations.
         */
        JavaPairDStream<String, KafkaMessage> windowedStream = zoneToMessagePairStream.window(WINDOW_DURATION, SLIDE_DURATION);

        /**
         * Group the messages in the windowed stream by the message zone
         */
        final JavaPairDStream<String, Iterable<KafkaMessage>> zoneToListOfEvents = windowedStream.groupByKey();

        /**
         * Iterate through each RDD associated with the windowed stream, then pass all
         * of the messages associated with each zone in the given window to the
         * AbnormalAccelerationDetection class which determins if the level of acceleration
         * within the given window for the given zone is abnormal
         */
        zoneToListOfEvents.foreachRDD(new Function2<JavaPairRDD<String, Iterable<KafkaMessage>>, Time, Void>()
        {
            public Void call(JavaPairRDD<String, Iterable<KafkaMessage>> rdd, Time time)
                    throws Exception
            {
                rdd.foreach(new VoidFunction<Tuple2<String, Iterable<KafkaMessage>>>()
                {
                    public void call(Tuple2<String, Iterable<KafkaMessage>> zoneAndMessageTuple)
                            throws Exception
                    {
                        String zone = zoneAndMessageTuple._1();
                        Iterable<KafkaMessage> messages = zoneAndMessageTuple._2();
                        KafkaMessageProducer kafkaMessageProducer = new KafkaMessageProducer(brokers, outgoingEventsTopic);
                        AbnormalAccelerationDetection abnormalAccelerationDetection = new AbnormalAccelerationDetection(
                                zone,
                                messages,
                                kafkaMessageProducer,
                                threshold
                        );
                        abnormalAccelerationDetection.sendEventOnAbnormalAcceleration();
                    }
                });
                return null;
            }
        });

        /**
         * Start the streaming job
         */
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    /**
     * A simple decoder which converts bytes to KafkaMessage objects
     * via a JSON ObjectMapper
     */
    public static class KafkaMessageDecoder implements Decoder<KafkaMessage>
    {
        public KafkaMessageDecoder(VerifiableProperties verifiableProperties)
        {
        /* This constructor must be present for successful compile. */
        }

        public KafkaMessage fromBytes(byte[] bytes)
        {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.readValue(bytes, KafkaMessage.class);
            }
            catch (IOException e) {
                System.out.println("Json processing failed for object: " + e.toString());
                throw new RuntimeException(e);
            }
        }
    }
}
