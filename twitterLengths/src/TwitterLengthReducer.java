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
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		resultKey.set(getRange(key.get()));
		context.write(resultKey, result);
	}

	private static String getRange(int lowerBound) {
		return String.format("%1$d-%2$d", lowerBound, lowerBound + 4);
	}

}
