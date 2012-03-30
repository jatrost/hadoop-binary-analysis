package io.covert.binary.analysis;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BuildSequenceFile extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new BuildSequenceFile(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		File inDir = new File(args[0]);
		Path name = new Path(args[1]);
		
		Text key = new Text();
		BytesWritable val = new BytesWritable();
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, name, Text.class, BytesWritable.class, CompressionType.RECORD);
		
		for(File file : inDir.listFiles())
		{
			if(!file.isFile())
			{
				System.out.println("Skipping "+file+" (not a file) ...");
				continue;
			}
			
			FileInputStream fileIn = new FileInputStream(file);
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream((int)file.length());
			int b;
			while(-1 != (b = fileIn.read()))
			{
				bytesOut.write(b);
			}
			fileIn.close();
			bytesOut.close();
			byte[] bytes = bytesOut.toByteArray();
			
			val.set(bytes, 0, bytes.length);
			key.set(file.getName());
			
			writer.append(key, val);
		}
		writer.close();
		
		return 0;
	}
}
