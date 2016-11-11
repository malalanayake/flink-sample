package taxi.stream;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

/**
 * http://robertmetzger.de/qcon-workshop/dataStream/3-handsOn.html
 *
 * Start an Apache Zookeeper instance =: ./bin/zookeeper-server-start.sh config/zookeeper.properties &
 * Start a Kafka instance =: ./bin/kafka-server-start.sh config/server.properties &
 * For Kafka Console : ./bin/kafka-console-consumer.sh  --topic cleansedRides --new-consumer --bootstrap-server localhost:9092
 */

/**
 * Java reference implementation for the "Ride Cleansing" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City.
 * The resulting stream is written to an Apache Kafka topic.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class RideCleansingToKafka {

    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String CLEANSED_RIDES_TOPIC = "cleansedRides";

    public static void main(String[] args) throws Exception {

        //ParameterTool params = ParameterTool.fromArgs(args);
        //String input = params.getRequired("input");

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("nycTaxiRides.gz", maxEventDelay, servingSpeedFactor));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        // write the filtered data to a Kafka sink
        filteredRides.addSink(new FlinkKafkaProducer09<>(
                LOCAL_KAFKA_BROKER,
                CLEANSED_RIDES_TOPIC,
                new TaxiRideSchema()));

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }


    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {

            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

}