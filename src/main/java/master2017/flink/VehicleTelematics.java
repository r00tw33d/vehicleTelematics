package master2017.flink;

import java.util.Iterator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
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
        String inFilePath = args[0];
        String outFolderPath = args[1];
        DataStreamSource<String> carsMapStream = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleStream;
        vehicleStream = transformCSVIntoTuple8(carsMapStream);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> speedRadarOutputStream;
        speedRadarOutputStream = filterByMaxSpeed(vehicleStream);

        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentOutputStream;
        accidentOutputStream = filterByAccident(vehicleStream);

        
        speedRadarOutputStream.writeAsCsv(outFolderPath + "/speedfines.csv");
        accidentOutputStream.writeAsCsv(outFolderPath + "/accidents.csv");
        
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

    private static SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filterByMaxSpeed(
            SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> singleOutputStreamOperator) {
        // FOTMAT: Time, VID, XWay, Seg, Dir, Spd
        return singleOutputStreamOperator.filter(
            new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                @Override
                public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                    // in.f2 is the Speed Field
                    return in.f2.compareTo(90) > 0;
                }
            });
    }

    private static SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filterByAccident(SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> vehicleStream) {
//        return vehicleStream.keyBy(1).countWindow(4, 1).trigger(new Trigger<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, GlobalWindow>() {
//            @Override
//            public TriggerResult onElement(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> t, long l, GlobalWindow w, Trigger.TriggerContext tc) throws Exception {
//                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//            }
//
//            @Override
//            public TriggerResult onProcessingTime(long l, GlobalWindow w, Trigger.TriggerContext tc) throws Exception {
//                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//            }
//
//            @Override
//            public TriggerResult onEventTime(long l, GlobalWindow w, Trigger.TriggerContext tc) throws Exception {
//                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//            }
//
//            @Override
//            public void clear(GlobalWindow w, Trigger.TriggerContext tc) throws Exception {
//                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//            }
//        })

        return vehicleStream.keyBy(1).countWindow(4, 1).apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple key, GlobalWindow w, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
                if (Iterables.size(input) < 4)
                    return;
                Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
                boolean hadMoved = false;
                Integer time1 = first.f0;
                Integer time2 = 0;
                while(iterator.hasNext()){
                    Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                    Integer posNext = next.f7;
                    hadMoved = true;
                    if (!posNext.equals(first.f7)) {
                        hadMoved = false;
                        break;
                    }
                    time2 = next.f0;
                }
                if (hadMoved)
                    // Time1, Time2, VID, XWay, Seg, Dir, Pos
                    out.collect(new Tuple7<>(time1, time2, first.f1, first.f3, first.f6, first.f5, first.f7));
            }
        });
                
          
    }

}
