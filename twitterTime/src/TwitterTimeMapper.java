import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.DateFormat;

public class TwitterTimeMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final Pattern regex = Pattern.compile("^([0-9]+);");
	private static final IntWritable one = new IntWritable(1);
	private static final DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
	
	private Text data = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String message = value.toString();
		//Match with regex to check for valid data
		Matcher matcher = regex.matcher(message);
		if (matcher.find()) {
			long epoch = Long.parseLong(matcher.group(1));
			Date tweetDate = new Date(epoch);
			System.out.println(epoch);
			String dateString = df.format(tweetDate);
			System.out.println(dateString);
			data.set(dateString);
			context.write(data, one);
		}
	}
}

