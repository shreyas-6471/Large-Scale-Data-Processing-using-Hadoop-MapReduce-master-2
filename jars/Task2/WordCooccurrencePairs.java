import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.*;

public class WordCooccurrencePairs {
public static class TextArrayWritable implements Writable,WritableComparable<TextArrayWritable>{
  private Class<? extends Writable> valueClass;
  private Text[] values;
 public TextArrayWritable() {
           //super(Text.class);
        }

 public TextArrayWritable(Class<? extends Writable> valueClass) {
    if (valueClass == null) {
      throw new IllegalArgumentException("null valueClass");
    }
    this.valueClass = valueClass;
  }

 public TextArrayWritable(Class<? extends Writable> valueClass, Text[] values) {
    this(valueClass);
    this.values = values;
  }

    public TextArrayWritable(Text[] TextPair){
    //super(Text.class);
    this(Text.class, new Text[TextPair.length]);
    for (int i = 0; i < TextPair.length; i++) {
        values[i] = TextPair[i];
    }
    }

    public void set(Text[] values){
        this.values = values;
    }

      public Text[] get() { return values; }
  
     public void write(DataOutput out) throws IOException {
     out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
      values[i].write(out);
    }
  }
 public void readFields(DataInput in) throws IOException {
    values = new Text[in.readInt()];          // construct values
    for (int i = 0; i < values.length; i++) {
      Text value = new Text();
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  @Override
    public String toString() {
        return "{word="+values[0]+
               " neighbor="+values[1]+"}";
    }


public int compareTo(TextArrayWritable o) {
         String thisValue0 = this.values[0].toString();
         String thisValue1 = this.values[1].toString();
         Text[] vals = o.get();
         String thatValue0 = vals[0].toString();
         String thatValue1 = vals[1].toString();
         if(thisValue0.compareTo(thatValue0)!=0)
         {
           return thisValue0.compareTo(thatValue0);
         }
         return thisValue1.compareTo(thatValue1);
       }

 public boolean equals(TextArrayWritable o){
    String thisValue0 = values[0].toString();
         String thisValue1 = values[1].toString();
         Text[] vals = o.get();
         String thatValue0 = vals[0].toString();
         String thatValue1 = vals[1].toString();
         return ((thisValue0.equals(thatValue0)) && (thisValue1.equals(thatValue1)));
 }


    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + values[0].hashCode();
        result = 31 * result + values[1].hashCode();
        return result;    }
    }

 public static class TokenizerMapper extends Mapper<Object, Text, TextArrayWritable, IntWritable>{
 private final static IntWritable one = new IntWritable(1);
   
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   String val = value.toString();
   String[] splited = val.split("\\s+");
   for(int i=0;i<splited.length-1;i++){
     for(int j=i+1;j<splited.length;j++){
       Text[] temp = new Text[2];
      TextArrayWritable output = new TextArrayWritable(temp);
       String token =splited[i];
       String neighbor = splited[j];
       temp[0] = new Text(token);
       temp[1] = new Text(neighbor);
       output.set(temp);
      context.write(new TextArrayWritable(output.get()), one);
     }
   }
   }
 }

public static class IntSumReducer extends Reducer<TextArrayWritable,IntWritable,TextArrayWritable,IntWritable> {
 private IntWritable result = new IntWritable();
 public void reduce(TextArrayWritable pair, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException
 {
 int sum = 0;
 for (IntWritable val : values) {
 sum += val.get();
 }
 result.set(sum);
context.write(pair, new IntWritable(result.get()));
 }
 }

public static void main(String[] args) throws Exception {
    try{
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "Word CooccurrencePairs");
 job.setJarByClass(WordCooccurrencePairs.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setCombinerClass(IntSumReducer.class);
 job.setReducerClass(IntSumReducer.class);
 job.setOutputKeyClass(TextArrayWritable.class);
 job.setOutputValueClass(IntWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
catch(Exception e){
e.printStackTrace();
}
}
}
