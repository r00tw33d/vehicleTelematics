/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 *
 * @author sylvers
 */
public class Vehicle {

    static SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> getSingleOutputStreamOperatorData() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    private Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> data;

    Vehicle(Integer valueOf, Integer valueOf0, Integer valueOf1, Integer valueOf2, Integer valueOf3, Integer valueOf4, Integer valueOf5, Integer valueOf6) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    
    /**
     * @return the data
     */
    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> data) {
        this.data = data;
    }
    
}
