package com.machine.classify.group.attributes.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 
 * @author Vipin Kumar
 *
 */
public class GroupAttributesDriver extends Configured implements Tool{
	
	
      public int run(String[] args) throws Exception
      {
            
            JobConf conf = new JobConf(getConf(), GroupAttributesDriver.class);
            conf.setJobName("GroupAttributeDriver");
            
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
           
            conf.setMapperClass(GroupAttributesMapper.class);
            conf.setReducerClass(GroupAttributesReducer.class);
            
            FileInputFormat.addInputPath(conf, new Path("/usr/hduser/output1/part-00000"));
            FileOutputFormat.setOutputPath(conf, new Path("/usr/hduser/output2/"));

            JobClient.runJob(conf);
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {	
    	  Configuration config=new Configuration();
    	  config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
    	  config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
          int res = ToolRunner.run(config, new GroupAttributesDriver(),args);
          System.exit(res);
      }
}

