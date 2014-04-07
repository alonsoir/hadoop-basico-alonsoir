package com.aironman.hadoop.SmSCallDetailRecords;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool 
{
   
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("App required params: {input file} {output dir}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		@SuppressWarnings("deprecation")
		final Job job = new Job(getConf(),"App");
		job.setJarByClass(App.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(SmsCDRMapper.class);
		job.setReducerClass(SmsCDRReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new App(), args);
	}
}
