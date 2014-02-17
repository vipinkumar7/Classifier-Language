package com.machine.classify.chisquare.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * 
 * @author Vipin Kumar
 *
 */
public class ChiSquareMapper extends MapReduceBase implements Mapper<LongWritable ,Text, Text, Text>{

	
	private Text attribute=new Text();
	
	/**
	 * 
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> context, Reporter reporter) throws IOException {

		String[] lineStat = value.toString().split("\t");
		if (isOdd(lineStat.length)) {
			attribute.set(lineStat[0]);
			context.collect(attribute, value);
		}
		
	}
	
	/**
	 * 
	 * @param i
	 * @return
	 */
	public boolean isOdd(int i)
	{
		return i%2!=0;
	}

	
}
