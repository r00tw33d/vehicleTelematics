package master2017.flink;

import java.util.Iterator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author sylvers
 */
public class VehicleTelematics {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        String inFilePath = args[0];
        String outFolderPath = args[1];
        DataStreamSource<String> carsMapStream = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleStream;
        vehicleStream = transformCSVIntoTuple8(carsMapStream);

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadarOutputStream;
        speedRadarOutputStream = filterByMaxSpeed(vehicleStream);

        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentOutputStream;
        accidentOutputStream = filterByAccident(vehicleStream);

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avgSpeedOutputStream;
        avgSpeedOutputStream = filterByAvgSpeed(vehicleStream);
        
        speedRadarOutputStream.writeAsCsv(outFolderPath + "/speedfines.csv", WriteMode.OVERWRITE);
        accidentOutputStream.writeAsCsv(outFolderPath + "/accidents.csv", WriteMode.OVERWRITE);
        avgSpeedOutputStream.writeAsCsv(outFolderPath + "/avgspeedfines.csv", WriteMode.OVERWRITE);
        
        try {
            env.execute("Speed Radar");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> transformCSVIntoTuple8(
            DataStreamSource<String> dataStreamSource) {
        return dataStreamSource.map(
            new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                @Override
                public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                    String[] fieldArray = in.split(",");
                    // TODO fieldArray length must be equal to 8
                    if (fieldArray.length < 8) return null;
                    Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out;
                    out = new Tuple8<>(Integer.valueOf(fieldArray[0]), Integer.valueOf(fieldArray[1]), Integer.valueOf(fieldArray[2]),
                            Integer.valueOf(fieldArray[3]),Integer.valueOf(fieldArray[4]),Integer.valueOf(fieldArray[5]),
                            Integer.valueOf(fieldArray[6]),Integer.valueOf(fieldArray[7]));
                    return out;
                }
            }
        );
    }

    private static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> filterByMaxSpeed(
            SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleStream) {
        // FOTMAT: Time, VID, XWay, Seg, Dir, Spd
        return vehicleStream.filter(
            new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                @Override
                public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                    // in.f2 is the Speed Field
                    return in.f2.compareTo(90) > 0;
                }
            }).map(
            new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                @Override
                public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple6) throws Exception {
                    //Time, VID, XWay, Seg, Dir, Spd
                    return new Tuple6<>(tuple6.f0, tuple6.f1, tuple6.f3, tuple6.f6, tuple6.f5, tuple6.f2);
                }
            });
    }

    private static SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filterByAccident(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleStream) {
        return vehicleStream.keyBy(1).countWindow(4, 1).apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple key, GlobalWindow w, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
                if (Iterables.size(input) < 4)
                    return;
                Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
                boolean hadMoved = false;
                Integer time1 = first.f0;
                Integer time2 = first.f0;
                Integer posNext;
                while(iterator.hasNext()){
                    Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                    time1 = time1 < next.f0 ? time1 : next.f0;
                    time2 = time2 > next.f0 ? time2 : next.f0;
                    posNext = next.f7;
                    hadMoved = true;
                    if (!posNext.equals(first.f7)) {
                        hadMoved = false;
                        break;
                    }
                }
                if (hadMoved)
                    // Time1, Time2, VID, XWay, Seg, Dir, Pos
                    out.collect(new Tuple7<>(time1, time2, first.f1, first.f3, first.f6, first.f5, first.f7));
            }
        });
                
          
    }

    private static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> filterByAvgSpeed(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleStream) {
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filteredVehicleStream;
        filteredVehicleStream = vehicleStream.filter(
            new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                @Override
                public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                    // in.f6 is the Seg Field, between 52 and 56
                    return in.f6.compareTo(52) >= 0 && in.f6.compareTo(56) <= 0;
                }
            });
        return filteredVehicleStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> t) {
                return t.f0 * 1000;
            }
        }).keyBy(1).window(EventTimeSessionWindows.withGap(Time.seconds(31))).apply(
                new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow w, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
                        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
                        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = null;
                        boolean exists52 = false;
                        boolean exists56 = false;
                        Integer time1 = Integer.MAX_VALUE;
                        Integer time2 = 0;
                        Double avgspeed = 0D;
                        while(iterator.hasNext()){
                            next = iterator.next();
                            avgspeed += next.f2;
                            exists52 = next.f6.equals(52) ? true : exists52;
                            exists56 = next.f6.equals(56) ? true : exists56;
                            time1 = time1 < next.f0 ? time1 : next.f0;
                            time2 = time2 > next.f0 ? time2 : next.f0;
                        }
                        avgspeed /= Iterables.size(input);
                        if (exists52 && exists56)
                            // Time1, Time2, VID, XWay, Dir, AvgSpeed
                            out.collect(new Tuple6<>(time1, time2, next.f1, next.f3, next.f5, avgspeed));
                    }
                }
        );
    }

}
