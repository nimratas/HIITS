package PA1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameReader;
import org.spark_project.jetty.server.NetworkTrafficServerConnector;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;

public class PA1 {

	public static void main(String[] args) {
		final String topic = args[0];

		// create Spark configuration
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("HIITS"));
		// read in text file and split each document into words
		JavaRDD<String> lines = sc.textFile("/dataLoc/titles-sorted.txt");
		JavaPairRDD<String,Long> lineCount = lines.zipWithIndex();
		JavaPairRDD<Long, String> indices = lineCount.mapToPair(new PairFunction<Tuple2<String,Long>, Long,String >(){
			public Tuple2<Long, String> call(Tuple2<String, Long> t) throws Exception {
				return t.swap();
			}			
		});


		JavaPairRDD<Long, String> rootSet = indices.filter(new Function<Tuple2<Long,String>, Boolean>(){

			public Boolean call(Tuple2<Long, String> arg0) throws Exception {
				String[] valSplit = arg0._2.split("_");
				return Arrays.asList(valSplit).contains(topic);
			}

		});

		JavaRDD<String> links = sc.textFile("/dataLoc/links-simple-sorted.txt");
		JavaPairRDD<Long, List<Long>>linksInfo = links.mapToPair(new PairFunction<String, Long, List<Long>>(){
			public Tuple2<Long, List<Long>> call(String s) throws Exception {
				String[] sArray = s.split(": ");
				try {
					Long key = Long.parseLong(sArray[0]);
					List<Long> values = new ArrayList();
					for(int i = 0 ; i < sArray[1].split(" ").length ; i++ ){
						values.add(Long.parseLong(sArray[1].split(" ")[i]));
					}
					return new Tuple2<Long, List<Long>>(key, values);
				}catch(NumberFormatException e){
					return null;
				}
			}
		});
		// break the list int single key value pairs
		JavaPairRDD<Long, Long> splitKV = linksInfo.mapToPair(new PairFunction<Tuple2<Long,List<Long>>, Long, Long>(){
			public Tuple2<Long, Long> call(Tuple2<Long, List<Long>> t) throws Exception {
				List<Long> list = t._2;
				for(Long v : list){
					return new Tuple2<Long, Long>(t._1, v);
				}
				return null;
			}
		});
		rootSet.coalesce(1,  true).saveAsTextFile("/rootoutput/rootset");
		//preparing root keys for join 
		JavaPairRDD<Long, Long> indKeys = rootSet.mapToPair(new PairFunction<Tuple2<Long,String>,Long,Long>(){
			public Tuple2<Long, Long> call(Tuple2<Long, String> t) throws Exception {
				return new Tuple2<Long, Long>(t._1 , null);
			}
		});
		// find outgoing links
		JavaPairRDD<Long,Tuple2<Long,Long>> outGoingLinks = splitKV.join(indKeys,10);
		JavaPairRDD<Long, Long> outLinks = 
				outGoingLinks.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Long>>,Long,Long>(){
					public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Long, Long>> t) throws Exception {
						return new Tuple2<Long, Long> (t._1, t._2._1);
					}
				});
		
		
		//find incoming links
		//from the splitKV RDD if I make values the keys and join with root
		// will give me incoming links
		JavaPairRDD<Long, Long> splitKVRev = splitKV.mapToPair(new PairFunction<Tuple2<Long,Long>,Long,Long>(){
			public Tuple2<Long, Long> call(Tuple2<Long, Long> t) throws Exception {
				return (t.swap());
			}
		});
		
		// key, incoming link
		JavaPairRDD<Long,Tuple2<Long,Long>> incomingLinks = splitKVRev.join(indKeys,10);
		JavaPairRDD<Long,Long> incomLinks = 
				incomingLinks.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Long>>,Long,Long>(){
					public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Long, Long>> arg0) throws Exception {
						return new Tuple2<Long, Long>(arg0._1, arg0._2._1);
					}
				});

		JavaPairRDD<Long,Tuple2<Iterable<Long>,Iterable<Long>>> baseSet = outLinks.cogroup(incomLinks);
		baseSet.coalesce(1,  true).saveAsTextFile("/baseoutput/baseSet");
		// calculation of hubs and authorities
		//put value 1 against each key in rootset
		JavaPairRDD<Long, Double> authRDD = indKeys.mapToPair(new PairFunction<Tuple2<Long,Long>,Long, Double>(){
			public Tuple2<Long, Double> call(Tuple2<Long, Long> arg0) throws Exception {
				return new Tuple2<Long, Double>(arg0._1, 1.0);
			}
		});

		JavaPairRDD<Long, Long> baseForCal = outLinks.union(incomLinks);
		JavaPairRDD<Long,Double> hubRDD = authRDD;
		JavaPairRDD<Long, Long> swapForHub = baseForCal.mapToPair(new PairFunction<Tuple2<Long,Long>, Long,Long>(){
			public Tuple2<Long, Long> call(Tuple2<Long, Long> arg0) throws Exception {
				return new Tuple2<Long, Long> (arg0._1, arg0._2).swap();
			}
		});

		int i =0;
		do{
			//create Scores
			// create authority scores, add auth scores of 
			JavaPairRDD<Long,Double> authScore = baseForCal.join(hubRDD, 10).
					mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Double>>, Long, Double>(){
						public Tuple2<Long, Double> call(Tuple2<Long, Tuple2<Long, Double>> arg0) throws Exception {
							return new Tuple2<Long,Double>(arg0._2._1, arg0._2._2);
						}
					}).reduceByKey(new org.apache.spark.api.java.function.Function2<Double, Double, Double>(){
						public Double call(Double arg0, Double arg1) throws Exception {
							return arg0 + arg1;
						}
					});

			final double sum = hubRDD.values().map(new Function<Double, Double>(){
				public Double call(Double arg0) throws Exception {
					return arg0;
				}
			}).reduce(new org.apache.spark.api.java.function.Function2<Double, Double, Double>(){
				public Double call(Double arg0, Double arg1) throws Exception {
					return arg0 + arg1;
				}
			});

			// normalize by dividing the auth values by sum
			authRDD = authScore.mapToPair(new PairFunction<Tuple2<Long,Double>, Long, Double>() {
				public Tuple2<Long, Double> call(Tuple2<Long, Double> arg0) throws Exception {
					return new Tuple2<Long, Double>(arg0._1, arg0._2/sum);
				}
			});		
			//			//calculating hubs
			//			//results in data like [1900, 1.0] etc
			//			// while testing open nowmAuth with yu
			//			//Calculate the hub score by the formula:
			//			//hub(a) = Authority(B)+ Authority(C) +Authority(D)
			//			// the goal is to obtain auth scores of incoming links
			//			JavaPairRDD<Long, Double> hubScore = createHubScore(swapForHub, authRDD);
			//			// Normalize hubScore
			//			//1. calculate sum of all authScores of authorties involved
			//			//2. divide individual auth score from above with hubSum
			JavaPairRDD<Long, Double> hubScore = swapForHub.join(authRDD, 10).
					mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Double>>, Long, Double>(){
						public Tuple2<Long, Double> call(Tuple2<Long, Tuple2<Long, Double>> arg0) throws Exception {
							return new Tuple2<Long, Double>(arg0._2._1, arg0._2._2);
						}
					}).reduceByKey(new org.apache.spark.api.java.function.Function2<Double, Double, Double>(){
						public Double call(Double arg0, Double arg1) throws Exception {
							return arg0 + arg1;
						}
					});
			final double sumHub = authRDD.values().map(new Function<Double, Double>(){
				public Double call(Double arg0) throws Exception {
					return arg0;
				}
			}).reduce(new org.apache.spark.api.java.function.Function2<Double, Double, Double>(){
				public Double call(Double arg0, Double arg1) throws Exception {
					return arg0 + arg1;
				}
			});
			// Normalize hubscores
			hubRDD = hubScore.mapToPair(new PairFunction<Tuple2<Long,Double>,Long, Double>(){
				public Tuple2<Long, Double> call(Tuple2<Long, Double> arg0) throws Exception {
					return new Tuple2 <Long, Double>(arg0._1, arg0._2/sumHub);
				}
			});
			i++;
		}
		while(i<=5);

		// Convert <Long, Double> to <Long, String> RDD
		JavaPairRDD<Long, String> authorities = authRDD.mapToPair(new PairFunction<Tuple2<Long,Double>, Long, String>(){
			public Tuple2<Long, String> call(Tuple2<Long, Double> arg0) throws Exception {
				return new Tuple2<Long, String>(arg0._1, arg0._2.toString());
			}
		});

		JavaPairRDD<String, String> authValues = authorities.join(indices, 10).
				mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,String>>, String, String>(){

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, String>> arg0) throws Exception {
						return new Tuple2<String, String>(arg0._2._1, arg0._2._2);
					}
				});
		// add ==> if the keyword exists in the values then save it

		JavaPairRDD<Long, String> hubs = hubRDD.mapToPair(new PairFunction<Tuple2<Long,Double>, Long, String>(){
			public Tuple2<Long, String> call(Tuple2<Long, Double> arg0) throws Exception {
				return new Tuple2<Long, String>(arg0._1, arg0._2.toString());
			}
		});


		JavaPairRDD<String, String> hubValues = hubs.join(indices, 10).
				mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,String>>, String, String>(){

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, String>> arg0) throws Exception {
						return new Tuple2<String, String>(arg0._2._1, arg0._2._2);
					}

				});
		JavaPairRDD<Double, String> hubValueRDD = hubValues.mapToPair(new PairFunction<Tuple2<String,String>, Double, String>(){

			public Tuple2<Double, String> call(Tuple2<String, String> arg0) throws Exception {
				return new Tuple2<Double,String>(Double.parseDouble(arg0._1), arg0._2);
			}
		}).sortByKey(false);

		JavaPairRDD<Double, String> authorityRDD = authValues.mapToPair(new PairFunction<Tuple2<String,String>, Double, String>(){
			public Tuple2<Double, String> call(Tuple2<String, String> arg0) throws Exception {
				return new Tuple2<Double,String>(Double.parseDouble(arg0._1), arg0._2);
			}
		}).sortByKey(false);
		authorityRDD.coalesce(1, true).saveAsTextFile("/output/urlValues");
		hubValueRDD.coalesce(1,true).saveAsTextFile("/hubOutput/hubValues");
	}
}
