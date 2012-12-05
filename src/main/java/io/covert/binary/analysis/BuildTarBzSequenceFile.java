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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;


import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
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

public class BuildTarBzSequenceFile extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new BuildTarBzSequenceFile(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		File inDir = new File(args[0]);
		Path name = new Path(args[1]);
		
		Text key = new Text();
		BytesWritable val = new BytesWritable();
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		if (!fs.exists(name)){
			fs.mkdirs(name);
		}
		for(File file : inDir.listFiles())
		{
			Path sequenceName = new Path(name, file.getName() + ".seq");
			System.out.println("Writing to " + sequenceName);
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, sequenceName, Text.class, BytesWritable.class, CompressionType.RECORD);
			if(!file.isFile())
			{
				System.out.println("Skipping "+file+" (not a file) ...");
				continue;
			}

		    final InputStream is = new FileInputStream(file); 
		    final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
		    TarArchiveEntry entry = null; 
		    while ((entry = (TarArchiveEntry)debInputStream.getNextEntry()) != null) {
		        if (!entry.isDirectory()) {
		        	
		            final ByteArrayOutputStream outputFileStream = new ByteArrayOutputStream(); 
		            IOUtils.copy(debInputStream, outputFileStream);
		            outputFileStream.close();
		            byte[] outputFile = outputFileStream.toByteArray();
		            val.set(outputFile, 0, outputFile.length);
		            
					MessageDigest md = MessageDigest.getInstance("MD5");
					md.update(outputFile);
					byte[] digest = md.digest();
					String hexdigest = "";
					for (int i=0; i < digest.length; i++) {
				           hexdigest += Integer.toString( ( digest[i] & 0xff ) + 0x100, 16).substring( 1 );
				    }
					key.set(hexdigest);
					writer.append(key, val);
		        }
		    }
		    debInputStream.close(); 
		    writer.close();
		}
		
		return 0;
	}
}


