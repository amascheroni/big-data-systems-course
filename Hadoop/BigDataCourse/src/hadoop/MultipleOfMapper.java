package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MultipleOfMapper class
 * 
 * @author Maximiliano Agustin Mascheroni
 */
public class MultipleOfMapper extends 
	Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Getting the target
		int target = Integer.parseInt(context.getConfiguration().get("target"));
		
		//Get the first line
		String line = value.toString();
		
		//Parse the line (text) to an Integer value
		int number = Integer.parseInt(line);
		
		//Get the remainder of the number
		int remainder = number % target;
		
		//Mapping, Key = number, Value = remainder
		IntWritable outputKey = new IntWritable(number);
		IntWritable outputValue = new IntWritable(remainder);
		
		//Write the pair <k,v>
		context.write(outputKey, outputValue);
		
	}
	
}
