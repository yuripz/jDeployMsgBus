package net.plumbing.msgbus.model;

import java.util.HashMap;
import java.util.Set;
//import org.springframework.context.annotation.Bean;



public  class MessageDirections {
   // public static HashMap<Integer, MessageDirectionsVO > AllMessageDirections;
    public static int RowNum=0;
    public static HashMap<Integer, MessageDirectionsVO > AllMessageDirections = new HashMap<Integer, MessageDirectionsVO >();

    public static int sizeAllMessageDirections() {
        return AllMessageDirections.size();
    }
    public static Set<Integer> keysAllMessageDirections() {
        return AllMessageDirections.keySet();
    }

}
