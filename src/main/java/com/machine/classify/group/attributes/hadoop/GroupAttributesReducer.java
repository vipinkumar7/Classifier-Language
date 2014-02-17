package com.machine.classify.group.attributes.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author Vipin Kumar
 *
 */
public class GroupAttributesReducer  extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterator<Text> value,OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		
		/**
		 * collect all the classes for the attribute
		 */
		StringBuilder  str=new StringBuilder();
		while(value.hasNext())
		{
			str.append(value.next()+"\t");
		}
		
		collector.collect(key, new Text(str.toString()));
	}

}
