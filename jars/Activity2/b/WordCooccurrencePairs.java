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
import java.util.HashMap;
import java.util.Collections;
import java.util.*;


public class WordCooccurrencePairs {
  public static String csvFile;
  public static HashMap<String,String> hashMap=new HashMap<String,String>();



 public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
 private Text locationT = new Text();
 int lineNum = 0;

 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   lineNum++;
   String valueStr = value.toString();
    int locEndPos = valueStr.indexOf(">");
    if(locEndPos>0){
   String location = valueStr.substring(0,locEndPos);
   location = location +"."+ Integer.toString(lineNum)+">";
   locationT = new Text(location);
   String content = valueStr.substring(locEndPos+1,valueStr.length());
   String val = content.toString();
   String[] splited = val.split("\\s+");
   for(int i=0;i<splited.length;i++){
     if(splited[i].endsWith(".") || splited[i].endsWith(",") || splited[i].endsWith("?") || splited[i].endsWith(";") || splited[i].endsWith(":") || splited[i].endsWith("!") || splited[i].endsWith("\"")){
       String tempSplit = splited[i];
       splited[i] = tempSplit.substring(0,tempSplit.length()-1);
     }
   }
  
   for(int k=0;k<splited.length-2;k++){
   	String token =splited[k];
   	if(!token.equals("") && !token.isEmpty()){
     for(int l=k+1;l<splited.length-1;l++){
      for(int m=l+1;m<splited.length;m++){
       Text[] temp = new Text[3];
      Text output = new Text();

       
       String neighbor1 = splited[l];
       String neighbor2 = splited[m];

if((!neighbor1.equals("") && !neighbor1.isEmpty())&&(!neighbor2.equals("") && !neighbor2.isEmpty())){
         temp[0] = new Text(token);
         temp[1] = new Text(neighbor1);
         temp[2] = new Text(neighbor2);
         output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
        context.write(new Text(output), locationT);
    }
       }
     }
   }
   }
 }
  }
 }

public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
 private Text result = new Text();
 public void reduce(Text pair, Iterable<Text> values,Context context ) throws IOException, InterruptedException
 {
 StringBuilder sb = new StringBuilder();
 for (Text val : values) {
sb.append(val.toString());
 }
 result = new Text(sb.toString());
 String[] wordPair = pair.toString().split(",");
 if(wordPair.length != 3){
  return;
 }
 String token = wordPair[0].substring(1,wordPair[0].length());
 String neighbor1 = wordPair[1];
 String neighbor2 = wordPair[2].substring(0,wordPair[2].length()-1);
 Text[] temp = new Text[3];
 Text output = new Text();
context.write(pair, result);
 //Normalisation
 token=token.replace("j", "i");
 token=token.replace("v", "u");
 neighbor1=neighbor1.replace("j", "i");
 neighbor1=neighbor1.replace("v", "u");
 neighbor2=neighbor2.replace("j", "i");
 neighbor2=neighbor2.replace("v", "u");
 //check for the lemma's in the hashMap
 if(!hashMap.containsKey(token) || !(hashMap.containsKey(neighbor1))|| !(hashMap.containsKey(neighbor2))) {
  return;
 }
 String lemmaString = hashMap.get(token);
 String lemma1Value = hashMap.get(neighbor1);
 String lemma2Value = hashMap.get(neighbor2);
  //If Lemmatizer.... <docid, [chapter#, line#]>
  if((lemmaString!=null) && !(lemmaString.isEmpty()) ){
  	String[] lemmaStringTokens = lemmaString.split(",");
  	

  for(int i=0;i<lemmaStringTokens.length;i++){
  	    temp[0] = new Text(lemmaStringTokens[i]);
    if((lemma1Value!=null) && !(lemma1Value.isEmpty())){
    String[] lemma1ValueTokens = lemma1Value.split(",");
    for(int j=0;j<lemma1ValueTokens.length;j++){
       temp[1] = new Text(lemma1ValueTokens[j]);
    if((lemma2Value!=null) && !(lemma2Value.isEmpty())){
          String[] lemma2ValueTokens = lemma2Value.split(",");

      for(int k=0;k<lemma2ValueTokens.length;k++){
      temp[2] = new Text(lemma2ValueTokens[k]);
     output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
     context.write(output, result);
    }
   
    }else{

      temp[2] = new Text(neighbor2);
      output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
     context.write(output, result);
     
    }
    
  	}

    }
    else{
    temp[1] = new Text(neighbor1);
    output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
    context.write(output, result);
  }

 }
 }
 else if((lemma1Value!=null) && (!(lemma1Value.isEmpty()))){
    String[] lemma1ValueTokens = lemma1Value.split(",");
  	

   for(int j=0;j<lemma1ValueTokens.length;j++){
  temp[0] = new Text(token);
  temp[1] = new Text(lemma1ValueTokens[j]);
  if((lemma2Value!=null) && !(lemma2Value.isEmpty())){
          String[] lemma2ValueTokens = lemma2Value.split(",");

      for(int k=0;k<lemma2ValueTokens.length;k++){
      temp[2] = new Text(lemma2ValueTokens[k]);
     output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
     context.write(output, result);
    }
    }else{

      temp[2] = new Text(neighbor2);
      output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
     context.write(new Text(output), result);
    }
  
  }
  return;
 }else if((lemma2Value!=null) && !(lemma2Value.isEmpty())){
String[] lemma2ValueTokens = lemma2Value.split(",");
    

   for(int j=0;j<lemma2ValueTokens.length;j++){
  temp[0] = new Text(token);
  temp[1] = new Text(neighbor1);

      temp[2] = new Text(lemma2ValueTokens[j]);
     output.set("<"+temp[0]+","+temp[1]+","+temp[2]+">");
     context.write(new Text(output), result);
    
    
  
  }
  return;
 }
return;

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
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 csvFile = args[2];

 BufferedReader br = null;
 String line = "";
 String cvsSplitBy = ",";

 try {
   br = new BufferedReader(new FileReader(csvFile));
   while ((line = br.readLine()) != null) {
    int index = line.indexOf(",");
   	String key = line.substring(0,index);
   	String content = line.substring(index+1,line.length());
	 hashMap.put(key,content);
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
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
catch(Exception e){
e.printStackTrace();
}
}
}
