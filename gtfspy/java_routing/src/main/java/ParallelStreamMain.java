import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toList;


public class ParallelStreamMain {

    public static void main(String[] args) {
        long start = System.nanoTime();
        /*
        Map<Integer, String> intToString = new HashMap<Integer, String>();
        for (int i = 0; i < 100; i++) {
            intToString.put(i, String.valueOf(i*i));
        }
        intToString.
        doubleList = Collections.synchronizedList(new ArrayList<Double>(Collections.nCopies(1000, null)) );
        IntStream ints = IntStream.range(0, 1000).parallel();
        ints.forEach(i -> doubles.set(i, inverse(i)));
        doubles.stream().forEachOrdered((d) -> {
            System.out.println(d);
        });
        long end = System.nanoTime();
        System.out.println((end-start)/(1_000_000_000.0));
        */
    }

    public static double inverse(int x) {
        try {
            sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return x;
    }

}
