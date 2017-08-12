package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleOfDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("target", args[0]);
		
		/* - Setting Job - */
		
		//Creating Job
		Job job = new Job(conf);
		job.setJarByClass(MultipleOfDriver.class);
		job.setJobName("Multiple of " + args[0] + " counter");
		
		//Adding Mapper and Reducer to the job
		job.setMapperClass(MultipleOfMapper.class);
	    job.setReducerClass(MultipleOfReducer.class);
	    
	    //Setting output
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    //Setting input
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    //Exit at finishing
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
