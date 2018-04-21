import org.apache.hadoop.io.*;


public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(IntWritable[] iw){
        this();
        set(iw);
    }
}