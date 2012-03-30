package io.covert.binary.analysis;

import io.covert.util.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class BinaryAnalysisMapper<K, V> extends Mapper<Text, BytesWritable, K, V>
{
	Logger LOG = Logger.getLogger(getClass());
	
	OutputParser<K, V> parser = null;
	String program;
	String[] args;
	int[] exitCodes;
	Map<String, Object> substitutionMap = new HashMap<String, Object>();
	long uuid = 0;
	File workingDir;
	String fileExtention;
	long timeoutMS;
	
	protected void setup(org.apache.hadoop.mapreduce.Mapper<Text,BytesWritable,K,V>.Context context) throws java.io.IOException ,InterruptedException 
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
		
		JobConf job = new JobConf(conf);
		
		// setup working dir
		workingDir = new File(new File(job.getJobLocalDir()).getParent(), context.getTaskAttemptID().toString());
		workingDir.mkdir();
		
		LOG.info("Working dir: ");
		for(File f : workingDir.listFiles())
		{
			LOG.info(f);
		}
		
		LOG.info("Working dir parent: ");
		for(File f : workingDir.getParentFile().listFiles())
		{
			LOG.info(f);
		}
		
		LOG.info("job.getLocalDirs(): ");
		for(String f : job.getLocalDirs())
		{
			LOG.info(f);
		}
		
		
		// prepare binary for exec
		
	}
	
	protected void map(Text key, BytesWritable value, org.apache.hadoop.mapreduce.Mapper<Text,BytesWritable,K,V>.Context context) throws java.io.IOException ,InterruptedException 
	{
		uuid++;
		
		long fileCreationOverheadMS = System.currentTimeMillis();
		File binaryFile = new File(workingDir, uuid+fileExtention);
		FileOutputStream fileOut = new FileOutputStream(binaryFile);
		fileOut.write(value.getBytes());
		fileOut.close();
		fileCreationOverheadMS = System.currentTimeMillis() - fileCreationOverheadMS;
		context.getCounter("STATS", "fileCreationOverheadMS").increment(fileCreationOverheadMS);
		
		substitutionMap.put("file", binaryFile.toString());
		
		context.getCounter("STATS", "Process execution attempts").increment(1);
		LOG.info("Running: "+program+" args="+Arrays.toString(args)+", substitutionMap="+substitutionMap+", exitCodes="+Arrays.toString(exitCodes)+", workingDir="+workingDir);
		
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
		context.getCounter("STATS", "programExecutionTimeMS").increment(programExecutionTimeMS);
		
		LOG.info("Process completed, elapsed="+programExecutionTimeMS+"ms, ExitCode="+exec.getExitCode()+", processStarted="+exec.isProcessStarted()+", processDestroyed="+exec.isProcessDestroyed());
		if(exec.getExecuteException() != null)
		{
			LOG.error(exec.getExecuteException());
		}
		
		if(exec.isProcessDestroyed())
		{
			context.getCounter("STATS", "Process destroyed").increment(1);
		}
		else
		{
			context.getCounter("STATS", "Process NOT destroyed").increment(1);
		}
		
		context.getCounter("EXIT CODES", ""+exec.getExitCode()).increment(1);
		
		if(exec.isProcessStarted())
		{
			context.getCounter("STATS", "Process started").increment(1);
			long parseOverheadMS = System.currentTimeMillis();
			// hand stdOut and stdErr to the parser
			parser.parse(key, value, exec.getStdOut(), exec.getStdErr());
			Collection<Entry<K, V>> values = parser.getResults();
			for(Entry<K, V> e : values)
			{
				context.write(e.getKey(), e.getValue());
			}
			parseOverheadMS = System.currentTimeMillis() - parseOverheadMS;
			context.getCounter("STATS", "parseOverheadMS").increment(parseOverheadMS);
		}
		else
		{
			context.getCounter("STATS", "Process NOT started").increment(1);
		}
		
		long fileDeletionOverheadMS = System.currentTimeMillis();
		binaryFile.delete();
		fileDeletionOverheadMS = System.currentTimeMillis() - fileDeletionOverheadMS;
		context.getCounter("STATS", "fileDeletionOverheadMS").increment(fileDeletionOverheadMS);
	}
	
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper<Text,BytesWritable,K,V>.Context context) throws java.io.IOException ,InterruptedException 
	{
		// cleanup working dir
	}
}
