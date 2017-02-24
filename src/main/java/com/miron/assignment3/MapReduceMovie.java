package com.miron.assignment3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

class MapReduceMovie {

    static void calculate(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "movieRating");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileUtils.deleteDirectory(new File(args[2]));

        job.waitForCompletion(true);

        PriorityQueue<Movie> queue = new PriorityQueue<>((o1, o2) ->
                Float.compare(o2.getRating(), o1.getRating())
        );
        Scanner scanner = new Scanner(new File(args[2] + File.separator + "part-r-00000"));
        while (scanner.hasNext()) {
            String movieId = scanner.next();
            float rating = scanner.nextFloat();
            Movie movie = new Movie(movieId, rating);
            queue.add(movie);
        }
        for (int i = 0; i < 10; i++) {
            Movie movie = queue.poll();
            Scanner movieScanner = new Scanner(new File(args[1]));
            while (movieScanner.hasNextLine()) {
                String line = movieScanner.nextLine();
                String movieId = line.substring(0, line.indexOf(","));
                if (movie.getId().equals(movieId)) {
                    movie.setName(line.substring(line.indexOf(",") + 1));
                    break;
                }
            }
            System.out.println(movie);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text word = new Text();
        private FloatWritable FloatWritable = new FloatWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                String movieID = token.substring(0, token.indexOf(","));
                word.set(movieID);
                int indexOf = token.lastIndexOf(",") + 1;
                int rating = Integer.parseInt(token.substring(indexOf, indexOf + 1));
                FloatWritable.set(rating);
                context.write(word, FloatWritable);
            }
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            sum = sum / count;
            context.write(key, new FloatWritable(sum));
        }
    }

    private static class Movie {

        private String id;
        private float rating;
        private String name;

        Movie(String id, float rating) {
            this.id = id;
            this.rating = rating;
        }

        String getId() {
            return id;
        }

        float getRating() {
            return rating;
        }

        void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "id='" + id + '\'' +
                    ", rating=" + rating +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}