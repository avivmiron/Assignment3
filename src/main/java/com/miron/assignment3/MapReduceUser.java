package com.miron.assignment3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.StringTokenizer;

class MapReduceUser {

    static void calculate(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "userReviews");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileUtils.deleteDirectory(new File(args[2]));

        job.waitForCompletion(true);

        PriorityQueue<User> queue = new PriorityQueue<>((o1, o2) ->
                Integer.compare(o2.getRatingCount(), o1.getRatingCount())
        );
        Scanner scanner = new Scanner(new File(args[2] + File.separator + "part-r-00000"));
        while (scanner.hasNext()) {
            String userId = scanner.next();
            int count = scanner.nextInt();
            User user = new User(userId, count);
            queue.add(user);
        }
        for (int i = 0; i < 10; i++) {
            System.out.println(queue.poll());
        }
    }

    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable intWritable = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                String userID = token.substring(token.indexOf(",") + 1, token.lastIndexOf((",")));
                word.set(userID);
                context.write(word, intWritable);
            }
        }
    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    private static class User {

        private String id;
        private int ratingCount;

        User(String id, int ratingCount) {
            this.id = id;
            this.ratingCount = ratingCount;
        }

        int getRatingCount() {
            return ratingCount;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id='" + id + '\'' +
                    ", ratingCount=" + ratingCount +
                    '}';
        }
    }
}