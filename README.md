I recently needed a quick way to analyze millions of small binary files (from 100K-19MB each) and
I wanted a scalable way to repeatedly do this sort of analysis.  I chose Hadoop as the platform,
and I built this little framework (really, a single MapReduce job) to do it.  This is very much a 
work in progress, and feedback and pull requests are welcome.

The main MapReduce job in this framework accepts a Sequence file of ```<Text, BytesWritable>``` where the 
```Text``` is a name and the ```BytesWritable``` is the contents of a file.  The framework unpacks the bytes of 
the ```BytesWritable``` to the local filesystem of the mapper it is running on, allowing the mapper to run
arbitrary analysis tools that require local filesystem access.  The framework then captures stdout and stderr from the
analysis tool/script and stores it (how it stores it is pluggable, see ```io.covert.binary.analysis.OutputParser```).

Building:

    mvn package assembly:assembly
    
Running:
    
    JAR=target/hadoop-binary-analysis-1.0-SNAPSHOT-job.jar
    
    # a local directory with files in it (directories are ignored for now)
    LOCAL_FILES=src/main/java/io/covert/binary/analysis/
    INPUT="dir-in-hdfs"
    OUTPUT="output-dir-in-hdfs"
    
    # convert a bunch of relatively small files into one sequence file (Text, BytesWritable)
    hadoop jar $JAR io.covert.binary.analysis.BuildSequenceFile $LOCAL_FILES $INPUT
    
    # Use the config properties in example.xml to basically run the wrapper.sh script on each file using Hadoop
    # as the platform for computation
    hadoop jar $JAR io.covert.binary.analysis.BinaryAnalysisJob -files wrapper.sh -conf example.xml $INPUT $OUTPUT

From example.xml:

      <property>
        <name>binary.analysis.program</name>
        <value>./wrapper.sh</value>
      </property>

      <property>
        <name>binary.analysis.program.args</name>
        <value>${file}</value>
      </property>

      <property>
        <name>binary.analysis.program.args.delim</name>
        <value>,</value>
      </property>

This block of example instructs the framework to run ```wrapper.sh``` using the args of ```${file}``` (where ```${file}```
is replaced by the unpacked filename from the Sequence File.  If multiple command line args are required,
they can be specified by appending a delimiter and then each arg to the value of the ```binary.analysis.program.args```
property



    
