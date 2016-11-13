import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwitterHashtag {

    public static void runJob(String[] input, String output, String cacheFile) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf);
        job.setJarByClass(TwitterHashtag.class);
        job.setMapperClass(TwitterHashtagMapper.class);
        job.setReducerClass(TwitterHashtagReducer.class);
	job.setCombinerClass(TwitterHashtagReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
	job.addCacheFile(new Path(cacheFile).toUri());
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length-2), args[args.length-2], args[args.length-1]);
    }

}
