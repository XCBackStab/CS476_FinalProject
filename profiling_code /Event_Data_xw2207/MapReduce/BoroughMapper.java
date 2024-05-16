import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BoroughMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the input value to a string
        String line = value.toString();
        
        // Skip processing the first row (header)
        if (key.get() == 0) {
            return;
        }
        
        // Split the line by comma, but only those not inside quotes
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); 

        // Check if columns array has sufficient elements to avoid IndexOutOfBoundsException
        if (columns.length > 4) {
            // Clean up the value in case it has quotes
            String columnValue = columns[0].trim().replaceAll("^\"|\"$", "");

            // Set the word and emit the key-value pair
            word.set(columnValue);
            context.write(word, one);
        }
    }
}
