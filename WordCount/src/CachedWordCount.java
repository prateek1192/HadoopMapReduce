
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class CachedWordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // HashSet as the set would be accessed multiple times
        HashSet<String> display = new HashSet();

        // Override method add words to cache
        @Override
        protected void setup (Context context)
                throws IOException, InterruptedException {
            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {

                URI[] localPaths = context.getCacheFiles();
                Path filePath = new Path(localPaths[0]);
                BufferedReader br = new BufferedReader(new FileReader(filePath.toString()));
                String lineTobeAdded = null;
                while((lineTobeAdded = br.readLine()) != null){
                    String[] wordsTobeAdded = lineTobeAdded.split(" ");
                    for(String wordToBeAdded : wordsTobeAdded ){
                        display.add(wordToBeAdded);
                    }
                }
            }
            super.setup(context);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            URI[] localPaths;
            localPaths = context.getCacheFiles();
            Path path = new Path(localPaths[0]);
            StringTokenizer itr = new StringTokenizer(value.toString());
            if (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                // Only add the word if it is in cached word set
                if (display.contains(word.toString())) {
                    System.out.println("Display contains word");
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            // New param which is the file to be cached
            System.err.println("Usage: wordcount <in> <out> <CachedFile>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(CachedWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        String fileName = otherArgs[2];
        // Make the cache file available
        job.addCacheFile(new Path(fileName).toUri());
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



