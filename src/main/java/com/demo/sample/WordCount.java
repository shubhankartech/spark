package com.demo.sample;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;
import scala.xml.Text;




public class WordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkDemo");
		JavaSparkContext context = new JavaSparkContext(conf);
//		JavaRDD<String> input = context.textFile("readme.txt",1);
//		input.filter(f)
//		operation(context);	
//		hiveOperations(context);

//		System.out.println(input.collect());
		HiveContext hiveContext = new HiveContext(context.sc());
//		SchemaRDD res = hiveContext.sql("");
//		SchemaRDD tables = hiveContext.table("foodmart.customer");
		DataFrame tables = hiveContext.sql("select * from foodmart.customer");
		System.out.println(tables.collect());
		context.close();
		
	}
	
	private static void getSplit(JavaSparkContext context){
		JavaRDD<String> input = context.textFile("/user/root/spark_sample/sample.txt");
		JavaRDD<String[]> res = input.map(new Function<String, String[]>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 8643757097470921608L;

			@Override
			public String[] call(String arg0) throws Exception {
				String[] res = split(arg0, "\\^");
				return res;
			}
			
			public  String[] split(String input,String charecter){
				ArrayList<String> matchList = new ArrayList<String>();
				Pattern ptrn = Pattern.compile(charecter);
				Matcher mtchr = ptrn.matcher(input);
				int index =0;
				
				while(mtchr.find()){
					String matched = input.substring(index,mtchr.start());
					matchList.add(matched);
					index=mtchr.end();
				}
				if(index == 0){
					return new String[]{input};
				}
				matchList.add(input.substring(index,input.length()));
				String[] result = new String[matchList.size()];
				return matchList.toArray(result);
			}
			
		});
		
		List<String[]> par = res.collect();
		for(String[] list:par){
			System.out.print(Arrays.toString(list));
		}
	}
	
	private static void hiveOperations(JavaSparkContext context) {
//		SparkSession hiveCtx = new SparkSession(context.sc());
//		String warehouseLocation = "/apps/hive/warehouse";
//		SparkSession hiveCtx = SparkSession
//				   .builder()
//				   .appName("SparkSessionZipsExample")
//				   .config("spark.sql.warehouse.dir", warehouseLocation)
//				   .enableHiveSupport()
//				   .getOrCreate();
//		//		DataFrame input = hiveCtx.jsonFile(inputFile);
////		input.registerTempTable("tweets");
//		try{
//			Dataset<Row> topTweets = hiveCtx.sql("set tlb={select count(*) from foodmart.customer}");
////			Dataset<Row> topTweets1 = hiveCtx.sql("select * from ${tlb}");
////			System.out.println("dataframe"+topTweets.show());
//			topTweets.show();
//		}catch(Exception exception){
//			exception.printStackTrace();
//			System.out.println("dataFrame error");
//		}
		
		
	}

	private static void operation(JavaSparkContext context) {
		JavaRDD<String> lines = context.parallelize(Arrays.asList("this is a test","the example is here","the error occurs"));
//		JavaRDD<String> filered = lines.filter(new Function<String,Boolean>(){
//			public Boolean call(String x){
//				return (x.contains("text")||x.contains("error"));
//			}
//		});
		JavaRDD<String> filered = lines.filter(new FilteredValue("error"));
		System.out.println("total"+filered.count()+" result"+filered.name());
		
		 List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<String, Integer>("hello", 1),
		            new Tuple2<String, Integer>("world", 1));
		JavaPairRDD<String, Integer> wordPair = context.parallelizePairs(tuples); 
		
		List<Tuple2<Integer, Integer>> numVals1 =  Arrays.asList( new Tuple2<Integer,Integer>(1,2),
				new Tuple2<Integer,Integer>(2,4),new Tuple2<Integer,Integer>(3,4),new Tuple2<Integer,Integer>(3,5));
		
		JavaPairRDD<Integer,Integer> numPairs1 = context.parallelizePairs(numVals1);
		
		List<Tuple2<Integer, Integer>> numVals2 =  Arrays.asList( new Tuple2<Integer,Integer>(2,2),
				new Tuple2<Integer,Integer>(4,4),new Tuple2<Integer,Integer>(5,4),new Tuple2<Integer,Integer>(6,5));
		
		JavaPairRDD<Integer,Integer> numPairs2 = context.parallelizePairs(numVals2);
		
		JavaPairRDD<Integer,Iterable<Integer>> groupedNum1 = numPairs1.groupByKey();
		
		System.out.println("grouped : "+groupedNum1.collect());
		
		List<Tuple2<String, String>> textVals2 =  Arrays.asList( new Tuple2<String,String>("hello","hello this is a world"),
				new Tuple2<String,String>("error","hello there is a error"),new Tuple2<String,String>("world","world hello there"),
				new Tuple2<String,String>("wow","wow i like it"));
		
		JavaPairRDD<String, String> textPairs2 = context.parallelizePairs(textVals2);
		
		textPairs2.filter(new FilterPair());
		
	    JavaRDD<String> input = context.textFile("/home/hduser/spark_samples/testweet.json");
//	    input.partitionBy(new HashPartitioner(2));
	    System.out.println("input: "+input.collect());
//	    JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(new LikesPandas());;
	    
//	    System.out.println("result:" + result.collect());
	    
	    JavaPairRDD<String,String> csv = context.wholeTextFiles("/home/hduser/spark_samples/csv/");
//	    JavaRDD<String[]> keyRDD = csv.flatMap(f)
	    
//	    JavaPairRDD<String, IntWritable> input = context.sequenceFile(path, Text.class, IntWritable.class)
	    
//		numVals1.par
		
//		textPairs2.sort
		
			
	}

	public static class ConvertToNativeType implements PairFunction<Tuple2<Text,IntWritable>,String, Integer>{
		public Tuple2<String,Integer> call(Tuple2<Text,IntWritable> record){
			return new Tuple2(record._1.toString(),record._2.get());
		}
	}
	public static class ParseLine implements Function<String,String[]>{
		@Override
		public String[] call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
	}
	  public static class LikesPandas implements Function<Person, Boolean> {
		    public Boolean call(Person person) {
		      return person.lovesPandas;
		    }
		  }
	
//	  public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
//		    public Iterator<Person> call(Iterator<String> lines) throws Exception {
//		      ArrayList<Person> people = new ArrayList<Person>();
//		      ObjectMapper mapper = new ObjectMapper();
//		      while (lines.hasNext()) {
//		        String line = lines.next();
//		        try {
//		          people.add(mapper.readValue(line, Person.class));
//		        } catch (Exception e) {
//		          // Skip invalid input
//		        }
//		      }
//		      return people.iterator();
//		    }
//		  }

	static class FilterPair implements Function<Tuple2<String,String>,Boolean> {

		@Override
		public Boolean call(Tuple2<String, String> keyValue) throws Exception {
		
			return keyValue._2.contains("world");
		}

		
		
	}
	
	public static class Person implements java.io.Serializable {
	    public String name;
	    public Boolean lovesPandas;
	  }
	
	static class FilteredValue implements Function<String, Boolean>{
		private String mValue;
		public FilteredValue(String value) {
			mValue = value;
		}
		
		public Boolean call(String x) { return x.contains(mValue); }
	}
}
