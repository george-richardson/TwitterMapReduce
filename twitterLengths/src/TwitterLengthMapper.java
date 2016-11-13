import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterLengthMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private IntWritable data = new IntWritable();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String text = value.toString();
		if (text.split(";").length == 4) {
			int lowerBound = (value.toString().length() / 5) * 5;
			data.set(lowerBound);
			context.write(data, one);
		}
	}
}

