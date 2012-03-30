/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.covert.binary.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BinaryAnalysisJob extends Configured implements Tool  {

	private static void usage(String msg)
	{
		System.err.println("Error: "+msg);
		System.err.println("Usage: hadoop jar JARFILE.jar "+BinaryAnalysisJob.class.getName()+" <inDir> <outDir>");
		System.err.println("    inDir  - HDFS input dir");
		System.err.println("    outDir - HDFS output dir\n");
		
		System.err.println("    Required Settings:");
		for(String name : requiredSettings)
		{
			System.err.println("        "+name);
		}
		System.err.println("    Optional Settings:");
		for(String name : optionalSettings)
		{
			System.err.println("        "+name);
		}
		
		System.exit(-1);
	}
	
	private static String[] requiredSettings = 
		{
			"binary.analysis.output.parser",
			"binary.analysis.program",
			"binary.analysis.program.args",
			"binary.analysis.program.exit.codes"
		};
	
	private static String[] optionalSettings = 
		{
			"binary.analysis.file.extention",
			"binary.analysis.execution.timeoutMS",
			"binary.analysis.program.args.delim",
		};
	
	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 2)
		{
			usage("");
		}
		
		String inDir = args[0];
		String outDir = args[1];
		
		Configuration conf = getConf();
		for(String name : requiredSettings)
		{
			if(conf.get(name) == null)
				usage("Missing required setting: "+name);
		}
		
		Job job = new Job(conf);
		job.setJobName(BinaryAnalysisJob.class.getName()+" inDir="+inDir+", outDir="+outDir);
		job.setJarByClass(getClass());
		
		job.setMapperClass(BinaryAnalysisMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(inDir));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));
		job.submit();
		
		int retVal = job.waitForCompletion(true)?0:1;
		return retVal;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BinaryAnalysisJob(), args);
	}
}
