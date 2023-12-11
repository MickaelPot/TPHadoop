
import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2_0 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\t");
			if (lines.length == 23) {
				if(!lines[8].isEmpty()) {
					if(   !lines[10].isEmpty() && !lines[11].isEmpty()) {
						if(isDouble(lines[10]) && isDouble(lines[11])) {
							Double latitude = Double.parseDouble(lines[10]) ;
							Double longitude = Double.parseDouble(lines[11]) ;
							Country country = Country.getCountryAt(latitude, longitude);
							if (country != null) {	
								String [] tags = lines[8].split(",");
								for (int i=0; i<tags.length; i++) {
									context.write(new Text(country.toString()), new Text(tags[i]));
								}	
							}
						}	
					}
				}
			}
		}
	}
	
	public static boolean isDouble(String entree) {
		try {
			Double.parseDouble(entree);
			return true;
		}catch(Exception e) {
			return false;
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (Text value : values) {
				for(Text autreValeur: values)
					if(value.toString().equals(autreValeur.toString())) {
						sum +=1;
					}
				context.write(value, new IntWritable(sum));
				sum=0;
				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question2_0.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Question 6
		job.setCombinerClass(MyReducer.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}