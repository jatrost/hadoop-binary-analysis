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
		System.err.println("Usage: hadoop jar JARFILE.jar "+BinaryAnalysisJob.class.getName()+" <inDir> <outDir>");
		System.err.println("    inDir  - HDFS input dir");
		System.err.println("    outDir - HDFS output dir");
		System.exit(-1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 2)
		{
			usage("");
		}
		
		String inDir = args[0];
		String outDir = args[1];
		
		Configuration conf = getConf();
		conf.set("binary.analysis.output.parser", SimpleOutputParser.class.getName());
		conf.set("binary.analysis.file.extention", ".dat");
		conf.setLong("binary.analysis.execution.timeoutMS", Long.MAX_VALUE);
		
		conf.set("binary.analysis.program", "md5sum");
		conf.set("binary.analysis.program.args", "${file}");
		conf.set("binary.analysis.program.args.delim", ",");
		conf.set("binary.analysis.program.exit.codes", "0,1");
		
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
