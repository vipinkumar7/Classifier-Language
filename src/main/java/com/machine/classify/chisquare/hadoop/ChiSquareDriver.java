package com.machine.classify.chisquare.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.machine.classify.group.attributes.hadoop.GroupAttributesDriver;
import com.machine.classify.model.AttributeClassData;
import com.machine.classify.model.ClassesStats;

/**
 * 
 * @author Vipin Kumar
 *
 */
public class ChiSquareDriver extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(ChiSquareDriver.class);
	
	


	public int run(String[] arg0) throws Exception {

		 JobConf conf = new JobConf(getConf(), GroupAttributesDriver.class);
         conf.setJobName("ChiSquareDriver");
         
         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(Text.class);
        
         conf.setMapperClass(ChiSquareMapper.class);
         conf.setReducerClass(ChiSquareReducer.class);
         
         FileInputFormat.addInputPath(conf, new Path("/usr/hduser/output2/part-00000"));
         FileOutputFormat.setOutputPath(conf, new Path("/usr/hduser/output3/"));

         JobClient.runJob(conf);
         return 0;
		
	}

	public static void main(String[] args) throws Exception {
		
		LOG.debug("Running Chisquare driver");
		ClassesStats.getClassesData();
		Configuration config = new Configuration();
		config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		int res = ToolRunner.run(config, new ChiSquareDriver(), args);
		
		Thread.sleep(res);
		AttributeClassData.intsertDataForAttributes();
	}
}
