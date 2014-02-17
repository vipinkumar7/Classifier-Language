package com.machine.classify.group.attributes.hadoop;

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
public class GroupAttributesMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text attribure = new Text();
	private Text attrData = new Text();

	public void map(LongWritable key, Text values,OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
		
		String line = values.toString();
		String[] arr = line.split("\t");
		if (arr.length == 3) {
			attribure.set(arr[0]);
			attrData.set(arr[1] + "\t" + arr[2]);
			output.collect(attribure, attrData);
		}

	}

}
