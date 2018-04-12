package examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by engry on 2018/4/12.
 */
public class JavaPi {
    public static void main(String[] args) {
        int NUM_SAMPLES = 1000;
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaPi");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < NUM_SAMPLES; i ++) {
            list.add(i);
        }
        long counts = jsc.parallelize(list).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x * x + y * y < 1;
        }).count();

        System.out.println(4.0 * counts / NUM_SAMPLES);
    }
}
