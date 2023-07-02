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
  import java.util.Map;
  import java.util.HashMap;
  import java.util.Iterator;
  import java.util.Set;
  public class WordCooccurrenceStripes {
  public static String csvFile;
    
  public static class MyMapWritable extends MapWritable implements Writable{
    @Override
       public String toString() {
           StringBuilder result = new StringBuilder();
           Set<Writable> keySet = this.keySet();

           for (Object key : keySet) {
               result.append("{" + key.toString() + " = " + this.get(key).toString() + "}");
           }
           return result.toString();
       }

       public int compareTo(MyMapWritable o) {
         String neighbor = "";
         String word = "";
         for (Map.Entry<Writable, Writable> extractData: o.entrySet()) {
            neighbor = extractData.getKey().toString();
         }
         for (Map.Entry<Writable, Writable> extractData1: this.entrySet()) {
             word = extractData1.getKey().toString();
         }
                return word.compareTo(neighbor);
              }


              public boolean equals(MyMapWritable o) {
                String neighbor = "";
                String word = "";
                for (Map.Entry<Writable, Writable> extractData: o.entrySet()) {
                   neighbor = extractData.getKey().toString();
                }
                for (Map.Entry<Writable, Writable> extractData1: this.entrySet()) {
                    word = extractData1.getKey().toString();
                }
                       return word.equals(neighbor);
                     }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, MyMapWritable>{
       MyMapWritable mapWritable = new MyMapWritable();
       int lineNum=0;
       HashMap<String,String> hashMap=new HashMap<String,String>();
       private Text word = new Text();

       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String val = value.toString();
         String[] splited = val.split("\\s+");
         int sum = 0;
         Text mToken = new Text();
         //private final static IntWritable one = new IntWritable(1);
         IntWritable sumW = new IntWritable(0);

         for(int i=0;i<splited.length-1;i++){
           MyMapWritable mapWritable = new MyMapWritable();
           for(int j=i+1;j<splited.length;j++){
             String token =splited[i];
             String neighbor = splited[j];
             mToken = new Text(token);
             Text mNeighbor = new Text(neighbor);
             if(mapWritable.containsKey(mNeighbor)){
              sum = ((IntWritable)mapWritable.get(mNeighbor)).get() + 1;
               mapWritable.remove(mNeighbor);
             }
              else{
              sum = 1;
              }
             sumW.set(sum);
             mapWritable.put(mNeighbor,sumW);
           }
           context.write(mToken, mapWritable);
         }
         }

   }

  public static class IntSumReducer extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {
   private IntWritable result = new IntWritable();
   public void reduce(Text word, Iterable<MyMapWritable> values,Context context ) throws IOException, InterruptedException
   {
    MyMapWritable y = new MyMapWritable();
   for (MyMapWritable val : values) {
     for (Map.Entry<Writable, Writable> extractData: val.entrySet()) {
       Text neighbor = new Text();
       int sum = 0;
       neighbor = new Text((Text)extractData.getKey());
       if(y.containsKey(neighbor)){
         IntWritable sumY = (IntWritable)y.get(neighbor);
         sum = sumY.get();
       }
       IntWritable x = (IntWritable)extractData.getValue();
       sum += x.get();
       result.set(sum);
       y.put(neighbor,new IntWritable(result.get()));
     }
   }
   for (Map.Entry<Writable, Writable> extractData: y.entrySet()) {
      MyMapWritable z = new MyMapWritable();
      z.put(extractData.getKey(),extractData.getValue());
      context.write(word, z);
}
   }
   }

  public static void main(String[] args) throws Exception {
      try{
   Configuration conf = new Configuration();
   Job job = Job.getInstance(conf, "Word CooccurrenceStripes");
   job.setJarByClass(WordCooccurrenceStripes.class);
   job.setMapperClass(TokenizerMapper.class);
   job.setCombinerClass(IntSumReducer.class);
   job.setReducerClass(IntSumReducer.class);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(MyMapWritable.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
   catch(Exception e){
    e.printStackTrace();
  }
  }
  }
