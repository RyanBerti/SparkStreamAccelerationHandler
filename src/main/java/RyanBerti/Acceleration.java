package RyanBerti;

import com.google.common.base.Splitter;

import java.util.Iterator;

/**
 * Created by admin on 12/9/15.
 */
public class Acceleration {

    private final double x;
    private final double y;
    private final double z;

    public Acceleration(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public static Acceleration valueOf(String value) {

        Iterable<String> splits = Splitter.on(",").limit(3).split(value);
        Iterator<String> iterator = splits.iterator();
        String[] vals = value.split(",");
        Acceleration a = new Acceleration(Double.valueOf(iterator.next()),
                Double.valueOf(iterator.next()),
                Double.valueOf(iterator.next()));
        return a;
    }

    public double getAbsAcceleration() {
        return Math.sqrt(x*x + y*y + z*z);
    }

    public double getY() {
        return y;
    }

    public double getZ() {
        return z;
    }

    public double getX() {

        return x;
    }
}
