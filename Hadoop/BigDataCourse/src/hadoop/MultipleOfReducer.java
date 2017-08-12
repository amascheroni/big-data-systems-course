package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MultipleOfReducer class
 * @author Maximiliano Agustin Mascheroni
 */
public class MultipleOfReducer extends
	Reducer<IntWritable, IntWritable, Text, IntWritable>{

	private int count;
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//Counting remainders equals to 0 (multiples)
		for(IntWritable value : values) {
			if (value.get() == 0) count++;
		}
	}
	
	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {

		//Generating outputs
		int target = Integer.parseInt(context.getConfiguration().get("target"));
		
		Text outputKey = new Text("The count of multiples of " + target + " is: ");
		context.write(outputKey, new IntWritable(count));
    }
	
}
