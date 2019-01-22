import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinOrder {

	public static class OrderClassifier extends Mapper<Object, Text, Text, Text> {

		private Text text = new Text();
		private Text pid = new Text();
		private Text result = new Text();

		public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {

			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String filename = inputSplit.getPath().getName();

			String tmp = value.toString();
			String[] lines = tmp.split("\n");
			for (String line : lines) {
				StringTokenizer itr = new StringTokenizer(line);
				String info = "";
				if (filename.startsWith("order")) {
					info += "order ";
					info += line.toString();
					if (!itr.hasMoreTokens()) return; else itr.nextToken();
					if (!itr.hasMoreTokens()) return; else itr.nextToken();
					if (!itr.hasMoreTokens()) return; else pid.set(itr.nextToken());
				}
				else {
					info += "product ";
					info += line.toString();
					if (!itr.hasMoreTokens()) return; else pid.set(itr.nextToken());
				}
				result.set(info);
				context.write(pid, result);
			}
		}
	}



	public static class OrderOutput extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text(), prefix = new Text();
		private String pName = "", type = "";
		private Integer cId = 0, price = 0;

		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			List<String> lines = new ArrayList<>();
			for (Text value : values) lines.add(value.toString());
			for (String line : lines) {
				StringTokenizer itr = new StringTokenizer(line);
				if (!itr.hasMoreTokens()) continue;
				type = itr.nextToken();
				if (type.equals("product")) {
					itr.nextToken();
					pName = itr.nextToken();
					cId = Integer.valueOf(itr.nextToken());
					price = Integer.valueOf(itr.nextToken());
					break;
				}
			}
			for (String line : lines) {
				StringTokenizer itr = new StringTokenizer(line);
				if (!itr.hasMoreTokens()) continue;
				type = itr.nextToken();
				if (type.equals("product")) continue;
				String info = pName + " " + String.valueOf(cId) + " " + String.valueOf(price);
				result.set(info);
				prefix.set(line);
				context.write(prefix, result);
			}

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "join order");
		job.setJarByClass(JoinOrder.class);

		job.setMapperClass(OrderClassifier.class);
		job.setReducerClass(OrderOutput.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}