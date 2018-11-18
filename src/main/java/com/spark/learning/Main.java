package com.spark.learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class Main {
    public static void main(String[] args) {
        String inputFileName = args[0];
        String outputFileName = args[1];

        SparkConf conf = new SparkConf()
                .setAppName("SparkLearning")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFileName);
        JavaRDD<String> words = input
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts = words
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        counts.saveAsTextFile(outputFileName);
    }
}
