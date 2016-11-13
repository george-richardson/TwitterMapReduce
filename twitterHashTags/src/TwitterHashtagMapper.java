import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import java.io.BufferedReader;
import java.util.Hashtable;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class TwitterHashtagMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Hashtable<String,String[]> countries;
	private final String[] prefixes = {"", "go", "team", "rio", "2016", "olympics"};
	private final Pattern regex = Pattern.compile("#(\\S+)");
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] sections = value.toString().split(";");
		if (sections.length == 4) {
			String message = sections[2];
			Matcher matcher = regex.matcher(message);
			String country = null;
			while(matcher.find()) {
				country = extractCountry(matcher.group(1));
				if (country != null) {
					data.set(country);
					context.write(data, one);
					break;
				}
			}
		}
	}

	private String extractCountry(String tagContent) {
		String result = null;
		String lowercaseTag = tagContent.toLowerCase();
		outerloop:
		for (String key: countries.keySet()) {
			for (String tag: countries.get(key)) {
				if (lowercaseTag.equals(tag)) {
					result = key;
					break outerloop;
				}
			}
		}
		return result;
	}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		countries = new Hashtable<String, String[]>();

		
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			
			br.readLine();

			while ((line = br.readLine()) != null) {
				String[] fields = line.split(",");
				String key = fields[0];
				ArrayList<String> tags = new ArrayList<String>();
				for (String field: fields) {
					String lowercase = field.toLowerCase().replaceAll("\\s+", "");
					for (String prefix: prefixes) {
						tags.add(prefix + lowercase);
					}
				}
				countries.put(key, tags.toArray(new String[0]));
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}
}

