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
import java.util.Scanner;
import java.util.regex.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.*;
import java.io.InputStream;
import java.io.InputStreamReader;

public class WordCountActivity1 {
public static String csvFile;
 public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
 int lineNum=0;
 HashMap<String,String> hashMap=new HashMap<String,String>();  
 private Text word = new Text();
 protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
		String[] keyValue = line.split(cvsSplitBy);
                StringBuilder valueString = new StringBuilder();
		for(int i=1;i<keyValue.length;i++)
		valueString.append(keyValue[i]+"||");
		hashMap.put(keyValue[0],valueString.toString());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String[] tokens = value.toString().split("\t");
if(tokens.length==2){
 String location = tokens[0];//<luc. 1.10>
 String[] locationTokens = location.toString().split("\\s+");
 StringTokenizer itr = new StringTokenizer(tokens[1]);
 lineNum++;

 while (itr.hasMoreTokens()) {
 String token =itr.nextToken();
 StringBuilder resultString = new StringBuilder();
 //Normalisation
 token=token.replace("j", "i");
 token=token.replace("v", "u");

 if(locationTokens.length == 3){
	resultString.append("<");
	resultString.append(token);
	resultString.append(" ");
	resultString.append("<");
	locationTokens[0]=locationTokens[0].replaceAll("<","");
	locationTokens[2]=locationTokens[2].replaceAll(">","");
	resultString.append(locationTokens[0]+" "+locationTokens[1]+",");
	resultString.append(" "+"["+locationTokens[2]+","+" "+lineNum+"]");
	resultString.append(">");
	resultString.append(">");
 }
 else if(locationTokens.length == 2){
	resultString.append("<");
	resultString.append(token);
	resultString.append(" ");
	resultString.append("<");
	locationTokens[0]=locationTokens[0].replace("<","");
	locationTokens[1]=locationTokens[1].replace(">","");
	resultString.append(locationTokens[0]+",");
	resultString.append(" "+"["+locationTokens[1]+","+" "+lineNum+"]");
	resultString.append(">");
	resultString.append(">");
 }



//check for the lemma's in the hashMap
String lemmaString = hashMap.get(token);

 //If Lemmatizer.... <docid, [chapter#, line#]>
String temp=" ";
 if(lemmaString!=null && !lemmaString.isEmpty() ){
 String[] lemmaStringTokens = lemmaString.toString().split("||");

 for(int j=0;j<lemmaStringTokens.length;j++){
//word.set(lemmaStringTokens[j]);
 word.set(temp);
 context.write(word, new Text(resultString.toString()));
}
}
else{
 word.set(temp);
// word.set(" ");
 context.write(word, new Text(resultString.toString()));
}
 }
 }
 }
}
public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
 private IntWritable result = new IntWritable();
 public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
 context.write(key, values);
 }
 }
public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "word count");
 job.setJarByClass(WordCountActivity1.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setCombinerClass(IntSumReducer.class);
 job.setReducerClass(IntSumReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 csvFile = args[2];
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}