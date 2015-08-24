package com.mvad.hadoop.tutorial.mr;

import com.mediav.data.log.CookieEvent;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.hadoop.thrift.ParquetThriftInputFormat;

import java.io.IOException;

/**
 * Created by guangbin on 15-07-07.
 * <p/>
 * This is an example of Using ParquetThriftInputFormat,
 * which explains reading CookieEvent Parquet File and count events number group by evenType ()
 */
public class CookieEventCountByEventType extends Configured implements Tool {

  public static class CookieEventMapper extends Mapper<Object, CookieEvent, IntWritable, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private IntWritable eventType = new IntWritable();

    @Override
    protected void map(Object key, CookieEvent value, Context context) throws IOException, InterruptedException {
      // Fill in Your code here
      int e = value.getEventType();
      eventType.set(e);
      context.getCounter("EventTypeCount", String.valueOf(e)).increment(1);
      context.write(eventType, one);
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

    private Text resultKey = new Text();
    private LongWritable resultValue = new LongWritable();

    public static String eventTypeInt2String(int i) {
      String res;
      switch (i) {
        case 99:
          res = new String("c");
          break;
        case 115:
          res = new String("s");
          break;
        case 116:
          res = new String("t");
          break;
        case 200:
          res = new String("u");
          break;
        default:
          res = new String("other");
      };
      return res;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      // Fill in Your code here
      long sum = 0;
      for (LongWritable value : values) {
        sum += value.get();
      }
      resultKey.set(eventTypeInt2String(key.get()));
      resultValue.set(sum);
      context.write(resultKey, resultValue);
    }
  }


  public int run(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: CookieEventCountByEventType <in> <out>");
      System.exit(2);
    }
    Path in = new Path(otherArgs[0]);
    Path out = new Path(otherArgs[1]);

    // setup job
    Job job = Job.getInstance(conf);
    job.setJobName("CookieEventCountByEventType");
    job.setJarByClass(CookieEventCountByEventType.class);
    job.setMapperClass(CookieEventMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // set InputFormatClass to be DelegateCombineFileInputFormat to Combine Small Splits
    job.setInputFormatClass(DelegateCombineFileInputFormat.class);
    DelegateCombineFileInputFormat.setCombinedInputFormatDelegate(job.getConfiguration(), ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.addInputPath(job, in);

    // be sure to set ParquetThriftInputFormat ReadSupportClass and ThriftClass
    ParquetThriftInputFormat.setReadSupportClass(job, CookieEvent.class);
    ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), CookieEvent.class);

    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true) ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CookieEventCountByEventType(), args);
  }


}
