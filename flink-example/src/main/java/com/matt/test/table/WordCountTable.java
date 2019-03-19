package com.matt.test.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Simple example for demonstrating the use of the Table API for a Word Count in Java.
 *
 * <p>This example shows how to:
 * - Convert DataSets to Tables - Apply group, aggregate, select, and filter operations
 */
public class WordCountTable {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<WC> input = env.fromElements(
            new WC("Hello", 1),
            new WC("Ciao", 1),
            new WC("Hello", 1));

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
            .groupBy("word")
            .select("word, frequency.sum as frequency")
            .filter("frequency = 2");

        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);

        result.print();
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO containing a word and its respective count.
     */
    public static class WC {
        public String word;
        public long frequency;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}

