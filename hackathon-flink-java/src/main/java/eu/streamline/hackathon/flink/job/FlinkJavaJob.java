package eu.streamline.hackathon.flink.job;

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.flink.operations.GDELTInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.Objects;

public class FlinkJavaJob {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJavaJob.class);

	public static void main(String[] args) {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String pathToGDELT = params.get("path");
		final String country = params.get("country", "USA");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<GDELTEvent> source = env
			.readFile(new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT).setParallelism(1);

		source.filter(new FilterFunction<GDELTEvent>() {
			@Override
			public boolean filter(GDELTEvent gdeltEvent) throws Exception {
				return Objects.equals(gdeltEvent.actor1Code_countryCode, country);
			}
		}).assignTimestampsAndWatermarks(
			new BoundedOutOfOrdernessTimestampExtractor<GDELTEvent>(Time.seconds(0)) {
				@Override
				public long extractTimestamp(GDELTEvent element) {
					return element.dateAdded.getTime();
				}
		}).keyBy(new KeySelector<GDELTEvent, String>() {
			@Override
			public String getKey(GDELTEvent gdeltEvent) throws Exception {
				return gdeltEvent.actor1Code_countryCode;
			}
		}).window(TumblingEventTimeWindows.of(Time.days(1))).fold(0.0,
			new FoldFunction<GDELTEvent, Double>() {
				@Override
				public Double fold(Double acc, GDELTEvent o) throws Exception {
					return acc + o.avgTone;
				}
			},
			new WindowFunction<Double, Tuple4<String, Double, Date, Date>, String, TimeWindow>() {
				@Override
				public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<Tuple4<String, Double, Date, Date>> out) throws Exception {
					Iterator<Double> it = input.iterator();
					out.collect(new Tuple4<>(key, it.next(), new Date(window.getStart()), new Date(window.getEnd())));
				}
		}).print();



		try {
			env.execute("Flink Java GDELT Analyzer");
		} catch (Exception e) {
			LOG.error("Failed to execute Flink job {}", e);
		}



	}

}
