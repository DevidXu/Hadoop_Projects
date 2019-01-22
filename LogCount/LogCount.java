import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogCount {


	public static class MobileLogMapper extends Mapper<Object, Text, Text, Text>{

		private Text phone = new Text();
		private Text info = new Text();

		public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
			String tmp = value.toString();
			String[] lines = tmp.split("\n");
			for (String line : lines) {
				int pos = line.indexOf(' ');
				String front = line.substring(0, pos);
				phone.set(front);
				info.set(line.substring(pos+1));
				context.write(phone, info);
			}
		}
	}



	public static class RegionPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String front = key.toString().substring(0, 3);
			int region = Integer.valueOf(front);
			return region % numReduceTasks;
		}
	}


	public static class InfoProcess extends Reducer<Text, Text, Text, Text>{

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			int upload = 0, download = 0;

			for (Text value : values) {
				int tUp = 0, tDown = 0;
				StringTokenizer itr = new StringTokenizer(value.toString());
				System.out.println(value.toString());
				if (!itr.hasMoreTokens()) continue;
				tUp = Integer.valueOf(itr.nextToken());
				if (!itr.hasMoreTokens()) continue;
				tDown = Integer.valueOf(itr.nextToken());

				upload += tUp; download += tDown;
			}

			result.set(String.valueOf(upload)+' '+String.valueOf(download));
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "log count");
		job.setJarByClass(LogCount.class);

		job.setMapperClass(MobileLogMapper.class);
		job.setCombinerClass(InfoProcess.class);
		job.setPartitionerClass(RegionPartitioner.class);
		job.setNumReduceTasks(4);
		job.setReducerClass(InfoProcess.class);

		// set map output format
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// set reduce output format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// specify input path and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// start the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}