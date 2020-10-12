package driver;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapper.FraudMapper;
import reducer.FraudReducer;
import writable.FraudWritable;

public class Driver
{
    public static void main(String[] args) throws IOException,  ClassNotFoundException,	 InterruptedException
    {

	//Setting up paths manually instead of command line arguments
	Path inputPath = new Path("hdfs://localhost:9000/user/jaime/input/EcommerceFraudPrediction/");
	Path outputDir = new Path("hdfs://localhost:9000/user/jaime/out/EcommerceFraudPrediction/");

	//Creating new job
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Fraud Detection");

	//Setting names of Jar, mapper, reducer ,  ....
	job.setJarByClass(Driver.class);
	job.setMapperClass(FraudMapper.class);
	job.setReducerClass(FraudReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FraudWritable.class);
	//Types of key value pairs
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	//paths for file input and output
	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputDir);
	
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	//termination status code
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
  	

