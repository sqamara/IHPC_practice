import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException; 
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StudentDetector {
    static boolean verbose = false;
    public static class ClassScoreMapper
            extends Mapper<Object, Text, Text, IntV3Writable>
        {

            private IntV3Writable nums = new IntV3Writable();
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

    public static class ClassSumScoreReducer
            extends Reducer<Text,IntV3Writable,Text,IntV3Writable> 
        {
            private IntV3Writable result = new IntV3Writable();

            public void reduce(Text key, Iterable<IntV3Writable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                result.zero();
                for (IntV3Writable val : values) {
                    result.sum(val);       
                }
                context.write(key, result);
                    }
        }

    public static class StudentClassScoreMapper
            extends Mapper<Object, Text, Text, StringArrayListWritable>
        {

            private Text student = new Text();
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
            {
                Configuration conf = context.getConfiguration();
                StringTokenizer itr = new StringTokenizer(value.toString());

                while (itr.hasMoreTokens()) {
                    student.set(itr.nextToken());
                    String className = itr.nextToken();
                    double cutoff = conf.getDouble(className, 100);
                    int score = Integer.parseInt(itr.nextToken());
                    if (score < cutoff) {
                        StringArrayListWritable classesWritable = 
                            new StringArrayListWritable(className);
                        context.write(student, classesWritable);
                    }   
                }
            }   
        }   
    public static class StudentClassesReducer
            extends Reducer<Text, StringArrayListWritable, Text, 
                            StringArrayListWritable> 
                {
                    public void reduce(Text key, 
                            Iterable<StringArrayListWritable> values, 
                            Context context) 
                        throws IOException, InterruptedException 
                    {
                        StringArrayListWritable allClasses = new StringArrayListWritable();

                        for (StringArrayListWritable array : values) {
                            allClasses.combine(array);
                            array.list.clear(); // this is needed, for some reason the iterator combines each StringArrayListWritable
                        }
                        context.write(key, allClasses);
                    }   
                }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Class Data");
        job.setJarByClass(StudentDetector.class);
        job.setMapperClass(ClassScoreMapper.class);
        job.setCombinerClass(ClassSumScoreReducer.class);
        job.setReducerClass(ClassSumScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntV3Writable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(verbose))
            System.exit(1);

        // make the output data of job1 part of configuration
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream is = fs.open(FileOutputFormat.getOutputPath(job).suffix("/part-r-00000"));
        
        String line =      is.readLine(); 
        while (line != null) {
            String[] tokens = line.split("\t");
            System.out.println(line + " " + tokens.length);
            String name = tokens[0];
            int count =        Integer.parseInt(tokens[1]); 
            int sum =          Integer.parseInt(tokens[2]); 
            int square_sum =   Integer.parseInt(tokens[3]);  

            double average = (double)(sum)/count;
            double square_sum_average = (double)(square_sum)/count;
            double standard_deviation = Math.sqrt(square_sum_average-
                    average*average);
            double cutoff = average-standard_deviation;

            conf.set(name, String.valueOf(cutoff));
            
            line =      is.readLine(); 
        }

        Job job2 = Job.getInstance(conf, "Student Data");
        job2.setJarByClass(StudentDetector.class);
        job2.setMapperClass(StudentClassScoreMapper.class);
        //        job2.setCombinerClass(StudentClassesCombiner.class);
        job2.setReducerClass(StudentClassesReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(StringArrayListWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(StringArrayListWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(verbose) ? 0 : 1);
    }

    private static class IntV3Writable implements Writable {
        int x = 0;
        int y = 0;
        int z = 0;

        public IntV3Writable() {}

        public IntV3Writable(int x, int y, int z) {
            this.set(x, y, z);
        }

        public void set(int x, int y, int z){
            this.x = x;      
            this.y = y;        
            this.z = z; 
        }

        public void zero(){
            this.set(0,0,0);
        }

        @Override
            public void readFields(DataInput in) throws IOException {
                this.x = in.readInt();
                this.y = in.readInt();
                this.z = in.readInt();
            }

        @Override
            public void write(DataOutput out) throws IOException {
                out.writeInt(x);
                out.writeInt(y);
                out.writeInt(z);
            }

        public void sum(IntV3Writable other) {
            this.x += other.x;
            this.y += other.y;
            this.z += other.z;
        }

        @Override
            public String toString() {
                return this.x + "\t" + this.y + "\t" + this.z;
            }
    }

    //    private static class StringIntWritable implements Writable {
    //        String s = "";
    //        int i = 0;
    //
    //        public StringIntWritable() {}
    //
    //        public StringIntWritable(String s, int i) {
    //            this.set(s, i);
    //        }
    //
    //        public void set(String s, int i){
    //            this.s = s;      
    //            this.i = i;        
    //        }
    //
    //        @Override
    //            public void readFields(DataInput in) throws IOException {
    //                this.s = in.readLine();
    //                this.i = in.readInt();
    //            }
    //
    //        @Override
    //            public void write(DataOutput out) throws IOException {
    //                out.writeBytes(s);
    //                out.writeInt(i);
    //            }
    //
    //        @Override
    //            public String toString() {
    //                return this.s + "\t" + this.i;
    //            }
    //    }
    public static class StringArrayListWritable implements Writable {
        ArrayList<String> list = new ArrayList<String>();
        public StringArrayListWritable(){}
        public StringArrayListWritable(String s){
            list.add(s);
        }
        public StringArrayListWritable(ArrayList<String> list){
            this.list = new ArrayList<String>(list);
        }
        public void combine(StringArrayListWritable other) {
            this.list.addAll(other.list);
        }
        @Override
            public void readFields(DataInput in) throws IOException {
                while (true) {
                    String input = in.readLine();
                    if (input != null)
                        this.list.add(input);
                    else
                        break;
                }
            }
        @Override
            public void write(DataOutput out) throws IOException {
                for (String s : list)
                    out.writeBytes(s);
            }
        @Override
            public String toString() {
                String toReturn = "";
                for (String s : list)
                    toReturn += s + "\t";
                return toReturn;
            }
    }
}
