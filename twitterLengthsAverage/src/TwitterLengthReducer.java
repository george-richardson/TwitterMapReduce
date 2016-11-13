import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwitterLengthReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	private Text resultKey = new Text();
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
	throws IOException, InterruptedException {
		int length = 0;
		long sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
			length++;
		}
		result.set((int) (sum / length));
		resultKey.set("Average");
		context.write(resultKey, result);
	}
}
