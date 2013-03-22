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
package io.covert.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * This is a utility MapReduce job that can be used to convert a file stored in HDFS to another file in HDFS.
 * 
 * Its primary use case was for converting a binary data file that was compressed with one format to a different compression format.
 * 
 * It outputs the data as GZipped.
 * 
 * Processing:
 * 		Each file provided will be streamed through a UNIX process (that reads from stdin and writes to stdout).
 * 
 * Input: 
 * 		text files in HDFS that contain a list of files in HDFS, one per line. The list of files will be processed.
 * 
 * Output: 
 * 		none
 * 
 * Side Effect:
 * 		files are written to HDFS in the same dir as the input files, but with a suffix of ".gz"
 * 		(There might be a better way to manage where the output files are written or how they're named, but this worked for me).
 * 
 * @author jtrost
 *
 */
public class FileFormatToConverterJob extends Configured implements Tool  {

	private static void usage(String msg)
	{
		System.err.println("Error: "+msg);
		System.err.println("Usage: hadoop jar JARFILE.jar "+FileFormatToConverterJob.class.getName()+" <inDir>");
		System.err.println("    inDir  - HDFS input dir");
		System.err.println("\n    stream.process.command - configuration option. The command to be run on each mapper. " );
		System.err.println("        This file needs to be predeployed (or the program needs to be installed on each mapper box).");
		System.err.println("        It also must read from stdin and write to stdout.");
		
		System.exit(-1);
	}
	
	static class ConvertMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>
	{
		static final Logger LOG = Logger.getLogger(ConvertMapper.class);
		
		@Override
		protected void map(LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
			LOG.info("Procesing file '"+value+"'");
			
			if(value.toString().trim().equals("")) 
			{
				LOG.info("Encountered empty string file name, skipping");
				return;
			}
			
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);			
			Path inputPath = new Path(value.toString());
			Path outputPath = new Path(value.toString()+".gz");
			
			if(!fs.exists(inputPath))
			{
				LOG.info("Input path does not exist, skipping "+inputPath);
				return;
			}

			OutputStream tmpDataOut;
			try {
				tmpDataOut = new GZIPOutputStream(fs.create(outputPath, false));
			} catch (Exception e1) {
				LOG.warn("Cannot Create output path for "+outputPath+", skipping "+inputPath);
				return;
			}
			
			final OutputStream dataOut = tmpDataOut;
			final FSDataInputStream dataIn = fs.open(inputPath);
			
			final Process proc = Runtime.getRuntime().exec(conf.get("stream.process.command"));
			final InputStream processErrorOutput = proc.getErrorStream();
			final InputStream processOutout = proc.getInputStream();
			final OutputStream processInput = proc.getOutputStream();
			
			final Thread stderrLogger = new Thread(){
				public void run() {
					
					try {
						BufferedReader r = new BufferedReader(new InputStreamReader(processErrorOutput));
						String line;
						while(null != (line = r.readLine()))
						{
							LOG.error("From Process: "+line);
						}
						r.close();
					} catch (IOException e) {
						LOG.error("Error reading from process stderr", e);
					}
				};
			};
			stderrLogger.start();
			
			final Thread progress = new Thread(){
				public void run() {
					try {
						while(true)
						{
							try {
								int code = proc.exitValue();
								LOG.info("Processed exited with code: "+code);
								break;
							} catch (IllegalThreadStateException e) {
								context.progress();
								Thread.sleep(5000);
							}
						}
					} catch (InterruptedException e) {}
					LOG.info("progress thread is exiting for file: "+value);
				};
			};
			progress.start();
			
			final Thread inputDataToProcessThread = new Thread(){
				public void run() {
					try {
						IOUtils.copy(dataIn, processInput);
					} catch (IOException e) {
						LOG.error("Error copying data for file: "+value, e);
						throw new RuntimeException("Error on file: "+value);
					}
					finally
					{
						try {
							dataIn.close();
						} catch (Exception e2) {}
						
						try {
							processInput.close();
						} catch (Exception e2) {}
					}
					LOG.info("inputDataToProcessThread: data copying thread is exiting for file: "+value);
					
				};
			};
			inputDataToProcessThread.start();
			
			final Thread processOutputToHdfsThread = new Thread(){
				public void run() {
					try {
						IOUtils.copy(processOutout, dataOut);
					} catch (Exception e) {
						LOG.error("Error copying data for file: "+value, e);
						throw new RuntimeException("Error on file: "+value);
					}
					finally
					{
						try {
							processOutout.close();
						} catch (Exception e2) {}
						
						try {
							dataOut.close();
						} catch (Exception e2) {}
					}
					LOG.info("processOutputToHdfsThread: data copying thread is exiting for file: "+value);
				};
			};
			processOutputToHdfsThread.start();
			
			
			proc.waitFor();
			
			inputDataToProcessThread.join();
			processOutputToHdfsThread.join();
			
			progress.interrupt();
			progress.join();
			
			stderrLogger.interrupt();
			stderrLogger.join();
			
			LOG.info("Done processing file: "+value);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 1)
		{
			usage("");
		}
		
		String inDir = args[0];
		
		Configuration conf = getConf();
		
		if(conf.get("stream.process.command") == null){
			conf.set("stream.process.command", "/opt/decompress.sh");
		}
				
		Job job = new Job(conf);
		job.setJobName(FileFormatToConverterJob.class.getName()+" inDir="+inDir);
		job.setJarByClass(getClass());
		
		job.setMapperClass(ConvertMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(inDir));
		
		job.setOutputFormatClass(NullOutputFormat.class);		
		job.submit();
		
		int retVal = job.waitForCompletion(true)?0:1;
		return retVal;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FileFormatToConverterJob(), args);
	}
}


