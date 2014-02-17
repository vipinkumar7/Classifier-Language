package com.machine.classify.learn.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Vipin Kumar
 *
 */
public class ConditionalProbabilityMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text attribute=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] lineStat = value.toString().split("\t");
		if (isOdd(lineStat.length)) {
			attribute.set(lineStat[0]);
			context.write(attribute, value);
		}
	}

	/**
	 * our data should always be odd if split is correct
	 * 
	 * @param i
	 * @return
	 */
	private boolean isOdd(int i) {
		return (i % 2) != 0;
	}

}
