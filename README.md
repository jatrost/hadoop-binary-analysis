Building:

    mvn package assembly:assembly
    
Running:
    
    JAR=target/hadoop-binary-analysis-1.0-SNAPSHOT-job.jar
    
    # a local directory with files in it (directories are ignored for now)
    LOCAL_FILES=src/main/java/io/covert/binary/analysis/
    INPUT="dir-in-hdfs"
    OUTPUT="output-dir-in-hdfs"
    
    # covert a bunch of relatively small files into one sequence file (Text, BytesWritable)
    hadoop jar $JAR io.covert.binary.analysis.BuildSequenceFile $LOCAL_FILES $INPUT
    
    # Use the config properties in example.xml to basically run the wrapper.sh script on each file using Hadoop
    # as the platform for computation
    hadoop jar $JAR io.covert.binary.analysis.BinaryAnalysisJob -files wrapper.sh -conf example.xml $INPUT $OUTPUT

This is very much a work in progress, and feedback and pull requests are welcome.
    
