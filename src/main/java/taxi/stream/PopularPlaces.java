package taxi.stream;

import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Popular Places" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 * <p>
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class PopularPlaces {

    public static void main(String[] args) throws Exception {

        // read parameters
        // ParameterTool params = ParameterTool.fromArgs(args);
        // String input = params.getRequired("input");

        final int popThreshold = 20;        // threshold for popular places
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", maxEventDelay, servingSpeedFactor));

        // find popular places
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularSpots = rides
                // remove all rides which are not within NYC
                .filter(new RideCleansing.NYCFilter())
                // match ride to grid cell and event type (start or end)
                .map(new GridCellMatcher())
                // partition by cell id and event type
                .<KeyedStream<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>>keyBy(0, 1)
                // build sliding window
                .timeWindow(Time.minutes(15), Time.minutes(5))
                // count ride events in window
                .apply(new RideCounter())
                // filter by popularity threshold
                .filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
                        return count.f3 >= popThreshold;
                    }
                })
                // map grid cell to coordinates
                .map(new GridToCoordinates());

        // print result on stdout
        popularSpots.print();

        env.setParallelism(1);
        // execute the transformation pipeline
        env.execute("Popular Places");
    }

    /**
     * Map taxi ride to grid cell and event type.
     * Start records use departure location, end record use arrival location.
     */
    public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

        @Override
        public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
            if (taxiRide.isStart) {
                // get grid cell id for start location
                int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                return new Tuple2<>(gridId, true);
            } else {
                // get grid cell id for end location
                int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                return new Tuple2<>(gridId, false);
            }
        }
    }

    /**
     * Counts the number of rides arriving or departing.
     */
    public static class RideCounter implements WindowFunction<
            Tuple2<Integer, Boolean>, // input type
            Tuple4<Integer, Long, Boolean, Integer>, // output type
            Tuple, // key type
            TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<Integer, Boolean>> values,
                Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {

            int cellId = ((Tuple2<Integer, Boolean>) key).f0;
            boolean isStart = ((Tuple2<Integer, Boolean>) key).f1;
            long windowTime = window.getEnd();

            int cnt = 0;
            for (Tuple2<Integer, Boolean> v : values) {
                cnt += 1;
            }

            out.collect(new Tuple4<>(cellId, windowTime, isStart, cnt));
        }
    }

    /**
     * Maps the grid cell id back to longitude and latitude coordinates.
     */
    public static class GridToCoordinates implements
            MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

        @Override
        public Tuple5<Float, Float, Long, Boolean, Integer> map(
                Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

            return new Tuple5<>(
                    GeoUtils.getGridCellCenterLon(cellCount.f0),
                    GeoUtils.getGridCellCenterLat(cellCount.f0),
                    cellCount.f1,
                    cellCount.f2,
                    cellCount.f3);
        }
    }

}
