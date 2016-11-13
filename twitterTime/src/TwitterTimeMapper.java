import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.DateFormat;

public class TwitterTimeMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private static final DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
	
	private Text data = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(";");
		if (fields.length == 4) {
			try {
				long epoch = Long.parseLong(fields[0]);
				Date tweetDate = new Date(epoch);
				String dateString = df.format(tweetDate);
				data.set(dateString);
				context.write(data, one);
			} catch(Exception e){}
		}
	}
}

