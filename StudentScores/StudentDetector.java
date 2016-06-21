import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;

public class StudentDetector {

    public static class ScoreMapper
            extends Mapper<Object, Text, Text, CompositeWritable>{

            private CompositeWritable nums = new CompositeWritable();
            private Text name = new Text();

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    itr.nextToken(); // skip student name
                    name.set(itr.nextToken()); //set name to class name
                    int score = Integer.parseInt(itr.nextToken());
                    nums.set(1, score, score*score);
                    context.write(name, nums);
                }
                    }
    }

    public static class SumScoreReducer
            extends Reducer<Text,CompositeWritable,Text,CompositeWritable> {
            private CompositeWritable result = new CompositeWritable();

            public void reduce(Text key, Iterable<CompositeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                result.zero();
                for (CompositeWritable val : values) {
                    result.sum(val);       
                }
                context.write(key, result);
                    }
    }

//    public static class ScoreMapper
//            extends Mapper<Object, Text, Text, CompositeWritable>{
//
//            private CompositeWritable nums = new CompositeWritable();
//            private Text name = new Text("All Classes");
//
//            public void map(Object key, Text value, Context context
//                    ) throws IOException, InterruptedException {
//
//                StringTokenizer itr = new StringTokenizer(value.toString());
//
//                while (itr.hasMoreTokens()) {
//                    itr.nextToken(); // skip student name and use "All Classes"
//                    int score = Integer.parseInt(itr.nextToken());
//                    nums.set(1, score, score*score);
//                    context.write(name, nums);
//                }   
//                    }   
//    }   
//
//    public static class SumScoreReducer
//            extends Reducer<Text,CompositeWritable,Text,CompositeWritable> {
//            private CompositeWritable result = new CompositeWritable();
//
//            public void reduce(Text key, Iterable<CompositeWritable> values,
//                    Context context
//                    ) throws IOException, InterruptedException {
//
//                for (CompositeWritable val : values) {
//                    result.sum(val);           
//                }   
//                context.write(key, result);
//                    }   
//    }   

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sum SquareSum");
        job.setJarByClass(StudentDetector.class);
        job.setMapperClass(ScoreMapper.class);
        job.setCombinerClass(SumScoreReducer.class);
        job.setReducerClass(SumScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompositeWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        double average = (double)(sum)/count;
//        double square_sum_average = (double)(square_sum)/count;
//        double standard_deviation = Math.sqrt(square_sum_average-
//                average*average);
//        double cutoff = average-standard_deviation;
//
//        Job job2 = Job.getInstance(conf, "get Studet");
//        job2.setJarByClass(StudentDetector.class);
//        job2.setMapperClass(ScoreMapper.class);
//        job2.setCombinerClass(SumScoreReducer.class);
//        job2.setReducerClass(SumScoreReducer.class);
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(CompositeWritable.class);
//        FileInputFormat.addInputPath(job2, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job2, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class CompositeWritable implements Writable {
        int count = 0;
        int sum = 0;
        int square_sum = 0;

        public CompositeWritable() {}

        public CompositeWritable(int count, int sum, int square_sum) {
            this.set(count, sum, square_sum);
        }

        public void set(int count, int sum, int square_sum){
            this.count      = count;      
            this.sum        = sum;        
            this.square_sum = square_sum; 
        }

        public void zero(){
            this.set(0,0,0);
        }

        @Override
            public void readFields(DataInput in) throws IOException {
                this.count      = in.readInt();
                this.sum        = in.readInt();
                this.square_sum = in.readInt();
            }

        @Override
            public void write(DataOutput out) throws IOException {
                out.writeInt(count);
                out.writeInt(sum);
                out.writeInt(square_sum);
            }

        public void sum(CompositeWritable other) {
            this.count      += other.count;
            this.sum        += other.sum;
            this.square_sum += other.square_sum;
        }

        @Override
            public String toString() {
                return this.count + "\t" + this.sum + "\t" + this.square_sum;
            }
    }
}
