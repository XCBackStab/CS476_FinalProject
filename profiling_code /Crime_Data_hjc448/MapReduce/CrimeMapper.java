import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting the CSV line
        String[] record = value.toString().split(",");
        // Assuming the borough is at index 0
        if (record.length > 0) {
            word.set(record[0]);
            context.write(word, one);
        }
    }
}
