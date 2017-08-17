package multipleof;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Main class to count the multiples of a given number
 * @author Maximiliano Agustin Mascheroni
 */
public class Main {

	public static void main(String[] args) {

		//Verify whether params are set
		try {
			if (args.length == 0 || args[0] == null || args[1] == null) {
				System.out.println("Path to jar or numbers file were not specified");
				System.out.println("Exiting...");
				System.exit(0);
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Path to jar or numbers file were not specified");
			System.out.println("Exiting...");
			System.exit(0);
		}

		//Setting Spark configuration and context
		SparkConf conf = new SparkConf().setAppName("Multiple of");
		JavaSparkContext context = new JavaSparkContext(conf);
		System.out.println("\n ======= MULTIPLES OF " + args[1] + " - SPARK IMPLEMENTATION ======= \n");
		
		//Reading the file
		System.out.println("Reading file " + args[0] + " ...");
		JavaRDD<Integer> numbersRDD = context.textFile(args[0]).map(
				new Function<String, Integer>() {
					public Integer call(String line) throws Exception {
						return Integer.parseInt(line);
					}
				}		
			);
		System.out.println("The file contains: " + numbersRDD.count() + " numbers \n");
		
		//Getting multiples of the given number
		int target = Integer.parseInt(args[1]);
		Function<Integer, Boolean> filterPredicate = e -> e % target == 0;
		JavaRDD<Integer> filteredList = numbersRDD.filter(filterPredicate);
		List<Integer> multiplesOf = filteredList.collect();
		System.out.println("The count of multiples of " + args[1] + " is: " + multiplesOf.size());
		
		//Closing context after use
		context.close();
	}
	
}