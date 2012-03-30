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

import io.covert.util.Utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.exec.ExecuteException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BinaryAnalysisMapper<K, V> extends Mapper<Text, BytesWritable, K, V>
{
	// Counter groups
	private static final String EXIT_CODES = "EXIT CODES";
	private static final String STATS = "STATS";

	// Counters
	private static final String FILE_DELETION_OVERHEAD_MS_COUNTER = "fileDeletionOverheadMS";
	private static final String PROGRAM_EXECUTION_TIME_MS_COUNTER = "programExecutionTimeMS";
	private static final String FILE_CREATION_OVERHEAD_MS_COUNTER = "fileCreationOverheadMS";
	private static final String PARSE_OVERHEAD_MS_COUNTER = "parseOverheadMS";
	private static final String PROCESS_NOT_DESTROYED_COUNTER = "Process NOT destroyed";
	private static final String PROCESS_NOT_STARTED_COUNTER = "Process NOT started";
	private static final String PROCESS_STARTED_COUNTER = "Process started";
	private static final String PROCESS_DESTROYED_COUNTER = "Process destroyed";


	Logger LOG = Logger.getLogger(getClass());
	
	OutputParser<K, V> parser = null;
	String program;
	String[] args;
	int[] exitCodes;
	Map<String, Object> substitutionMap = new HashMap<String, Object>();
	long uuid = 0;
	File workingDir;
	File dataDir;
	String fileExtention;
	long timeoutMS;
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		try {
			parser = (OutputParser<K, V>) Class.forName(conf.get("binary.analysis.output.parser")).newInstance();
		} catch (Exception e) {
			throw new IOException("Could create parser", e);
		}
		
		fileExtention = conf.get("binary.analysis.file.extention", ".dat");
		timeoutMS = conf.getLong("binary.analysis.execution.timeoutMS", Long.MAX_VALUE);
		program = conf.get("binary.analysis.program");
		args = conf.get("binary.analysis.program.args").split(conf.get("binary.analysis.program.args.delim", ","));
		String[] codes = conf.get("binary.analysis.program.exit.codes").split(",");
		exitCodes = new int[codes.length];
		for(int i = 0; i < codes.length; ++i)
		{
			exitCodes[i] = Integer.parseInt(codes[i]);
		}
		
		workingDir = new File(".").getAbsoluteFile();
		dataDir = new File(workingDir, "_data");
		dataDir.mkdir();
		logDirContents(workingDir);
		
		File programFile = new File(workingDir, program);
		if(programFile.exists())
		{
			LOG.info("Program file exists in working directory, ensuring executable and readable");
			programFile.setExecutable(true);
			programFile.setReadable(true);
		}
	}
	
	private void logDirContents(File dir)
	{
		if(LOG.isDebugEnabled())
		{
			LOG.debug("dir="+dir.getName());
			for(File f : dir.listFiles())
			{
				LOG.debug(f);
				if(f.isDirectory())
				{
					LOG.debug(Arrays.toString(f.listFiles()).replace(",", "\n"));
				}
			}
		}
	}
	
	protected void map(Text key, BytesWritable value, Context context) 
		throws java.io.IOException ,InterruptedException 
	{
		uuid++;
		File binaryFile = new File(dataDir, uuid+fileExtention);
		writeToFile(value, binaryFile, context);
		substitutionMap.put("file", binaryFile.getPath());
		
		execute(key, value, context);
		
		long fileDeletionOverheadMS = System.currentTimeMillis();
		binaryFile.delete();
		fileDeletionOverheadMS = System.currentTimeMillis() - fileDeletionOverheadMS;
		context.getCounter(STATS, FILE_DELETION_OVERHEAD_MS_COUNTER).increment(fileDeletionOverheadMS);
	}
	
	protected void execute(Text key, BytesWritable value, Context context) 
		throws ExecuteException, IOException, InterruptedException
	{
		context.getCounter(STATS, "Process execution attempts").increment(1);
		LOG.info("Running: "+program+" args="+Arrays.toString(args)+", substitutionMap="+substitutionMap+
				 ", exitCodes="+Arrays.toString(exitCodes)+", workingDir="+workingDir);
		
		long programExecutionTimeMS = System.currentTimeMillis();
		ExecutorThread exec = new ExecutorThread(program, args, substitutionMap, exitCodes, timeoutMS, workingDir);
		exec.start();
		while(exec.isAlive())
		{
			context.progress();
			Utils.sleep(100);
		}
		exec.join();
		programExecutionTimeMS = System.currentTimeMillis() - programExecutionTimeMS;
		context.getCounter(STATS, PROGRAM_EXECUTION_TIME_MS_COUNTER).increment(programExecutionTimeMS);
		context.getCounter(EXIT_CODES, ""+exec.getExitCode()).increment(1);
		
		LOG.info("Process completed, elapsed="+programExecutionTimeMS+"ms, ExitCode="+exec.getExitCode()+
				 ", processStarted="+exec.isProcessStarted()+", processDestroyed="+exec.isProcessDestroyed());
		
		if(exec.getExecuteException() != null)
			LOG.error(exec.getExecuteException());
		
		if(exec.isProcessDestroyed())
			context.getCounter(STATS, PROCESS_DESTROYED_COUNTER).increment(1);
		else
			context.getCounter(STATS, PROCESS_NOT_DESTROYED_COUNTER).increment(1);
		
		
		if(exec.isProcessStarted())
		{
			context.getCounter(STATS, PROCESS_STARTED_COUNTER).increment(1);
			parse(key, value, exec.getStdOut(), exec.getStdErr(), context);
		}
		else
		{
			context.getCounter(STATS, PROCESS_NOT_STARTED_COUNTER).increment(1);
		}
	}
	
	protected void parse(Text key, BytesWritable value, ByteArrayOutputStream stdOut,ByteArrayOutputStream stdErr, Context context) 
		throws IOException, InterruptedException
	{
		long parseOverheadMS = System.currentTimeMillis();
		parser.parse(key, value, stdOut, stdErr);
		for(Entry<K, V> e : parser.getResults())
		{
			context.write(e.getKey(), e.getValue());
		}
		parseOverheadMS = System.currentTimeMillis() - parseOverheadMS;
		context.getCounter(STATS, PARSE_OVERHEAD_MS_COUNTER).increment(parseOverheadMS);
	}
	
	protected void writeToFile(BytesWritable value, File binaryFile, Context context) throws IOException
	{
		long fileCreationOverheadMS = System.currentTimeMillis();

		FileOutputStream fileOut = new FileOutputStream(binaryFile);
		fileOut.write(value.getBytes());
		fileOut.close();
		fileCreationOverheadMS = System.currentTimeMillis() - fileCreationOverheadMS;
		context.getCounter(STATS, FILE_CREATION_OVERHEAD_MS_COUNTER).increment(fileCreationOverheadMS);
	}
	
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper<Text,BytesWritable,K,V>.Context context) 
		throws java.io.IOException, InterruptedException 
	{
		
	}
}
