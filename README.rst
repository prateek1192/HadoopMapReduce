Overview
--------

The project is a collection of Java Codes for running Hadoop
Map Reduce tasks.

Using these scripts
----------------------

Before you begin
++++++++++++++++

1) Install java on your namenode. The above scripts are only 
compatible with Java > 7


How to Run
++++++++++++++++++++++++++


- hadoop jar hadoop_data/WordCount.jar SingleWordCount <input> <output>

 

- hadoop jar hadoop_data/WordCount.jar DoubleWordCount <input> <output>

 

- hadoop jar local/hadoop_data/WordCount.jar CachedWordCount <input> <output> <cache-file>

 
where 
<input> is the path to the input file
<outut> is the path to the output file
<cache-file> has the words which would only be used if this option is enabled
