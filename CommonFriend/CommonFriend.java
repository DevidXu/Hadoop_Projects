import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CommonFriend {

	public static class Contributor extends Mapper<Object, Text, Text, Text> {

		private Text pair = new Text(), owner = new Text();

		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			String tmpFile = value.toString();
			String[] lines = tmpFile.split("\n");

			for (String line : lines) {
				Integer pos = line.indexOf(':');
				if (pos == -1) continue;	// invalid line, please skip it
				String name = line.substring(0, pos);
				owner.set(name);
				
				line = line.substring(pos+1);
				String[] friends = line.split(",");
				int len = friends.length;
				for (int i=0;i<len;i++)
					for (int j=0;j<i;j++) {
						String A = friends[i], B = friends[j];
						if (A.compareTo(B)>0) {
							String tmp = A; A = B; B = tmp;
						}
						String tmpKey = A+"-"+B;
						pair.set(tmpKey); 
						context.write(pair, owner);
					}
			}
		}

	}


	public static class Classifier extends Reducer<Text, Text, Text, Text> {

		private Text friendPair = new Text(), info = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
			String result = "";
			for (Text value : values) {
				String name = value.toString();
				if (result.length()==0) result += name;
				else result += ","+name;
			}

			String pair = key.toString() + ":";
			friendPair.set(pair);
			info.set(result);

			context.write(friendPair, info);
		}

	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Common Friend");

		job.setMapperClass(Contributor.class);
		job.setReducerClass(Classifier.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}