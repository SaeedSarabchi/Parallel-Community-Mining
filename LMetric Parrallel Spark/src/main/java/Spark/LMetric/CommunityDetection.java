package Spark.LMetric;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class CommunityDetection
{
	public static void main(String[] args) throws Exception
	{

		String inDir = args[0];
		String outDir = args[1];
		int startPointSeq = Integer.parseInt( args[2] );
		int startPointSpark = Integer.parseInt( args[3] );
		int thresholdSeq = Integer.parseInt( args[4] );
		// hadoop threshold
		int threshold = Integer.parseInt( args[5] );
		
		// Delete Main output path if it exists
		FileSystem mfs = FileSystem.get( URI.create(outDir),new Configuration() );
		Path mainOutPath = new Path( outDir );  
		if( mfs.exists(mainOutPath) )
		{
			if( !mfs.delete(mainOutPath,true) )
			{
				System.out.println( "\n\nDelete output file:"+args[1]+" failed!\n\n" );
				return;
			}
		}
		
		if ( thresholdSeq > 0 )
		{
			//Find Community Sequentially and compute time
			SequentialL sl = new SequentialL();
			long begin = System.currentTimeMillis();
			sl.findLCommunitySeqSpark( startPointSeq, inDir, outDir, thresholdSeq );
			long end = System.currentTimeMillis();
			System.out.println( "Sequential program time:" + String.valueOf( (end-begin)/1000f ) ); 
			
			try
	        {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get( URI.create(outDir),conf );
				FSDataOutputStream out = fs.create( new Path(outDir+"/"+"SequentialTime.dat") );			
				out.writeUTF( "Sequential program time:" + String.valueOf( (end-begin)/1000f ) );
		        out.close();
		    }
	        catch (Exception e) 
	        {
	        	e.printStackTrace();
		    }
		}
		
		
		if ( threshold > 0 )
		{
			// Initialize variables
			
			long begin2 = System.currentTimeMillis();
			
			GlobalVariables gv = new GlobalVariables();
			Map<Integer,Boolean> map = new HashMap<Integer,Boolean>(); 
		

			// Count the out degrees of all the points
			countStartPoint( inDir, outDir );
			
			// Get all points information into map to record whether a node is visited
			getAllPoints( outDir, map );
			map.put( startPointSpark, true );
			
			//index on Remaining Nodes
			int indexRemaining=0;
			
			System.out.println( "\n Start of findLLocalCommunity \n");
		
			for( int i = 0; i < threshold; i++ )
			{
				boolean over = true;
				//System.out.println( "\n In Loop Before findMLocalCommunity  \n");
				
				// Find local community for a particular start point
				LLocalCommunity.findLLocalCommunity( startPointSpark, inDir, outDir, i, gv, map );
				
				//If No Community Found, Make the visited feature of startNode false.
				if(gv.commPoints.size()==0)
					map.put(startPointSpark, false);
	
		        
				
				// Set the status of all community nodes to visited
				for( int j = 0; j < gv.commPoints.size(); j++ )
				{
					int point = gv.commPoints.get(j);
					map.put( point, true );
					//out.writeInt( point );
				}
				
				
				
				// If there is still node that has not been visited, set it to start point
				ArrayList<Integer> al = new ArrayList<Integer>();
				for( Map.Entry<Integer,Boolean> entry: map.entrySet() )
				{
					
					if( entry.getValue() == false )
					{
						al.add( entry.getKey() );
					}
				}
				
				if ( al.size() > 0 )
				{
					Collections.sort(al);
					
					if(gv.commPoints.size()>0)
						indexRemaining = 0;
					else
						indexRemaining++;
					
					//If none of the Nodes contribute to Community Discovery, Then Finish
					if(indexRemaining>=al.size()-1)
					{
						over = true;
						break;
					}
					
					startPointSpark = al.get(indexRemaining);
					map.put( startPointSpark, true );
					over = false;
				}

	
				// If no more nodes to visit
				if( over == true )	
					break;
			}
	
			long end2 = System.currentTimeMillis();
			System.out.println( "Spark program spend time:" + String.valueOf( (end2-begin2)/1000f ) ); 
			
			try
	        {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get( URI.create(outDir),conf );
				FSDataOutputStream out = fs.create( new Path(outDir+"/"+"ParallelTime.dat") );			
				out.writeUTF( "Parallel program time:" + String.valueOf( (end2-begin2)/1000f ) );
		        out.close();
		    }
	        catch (Exception e) 
	        {
	        	e.printStackTrace();
		    }
		
		}
		
	}
	
	

	// Count the out degrees of all the points and write it to outDir/count
	public static void countStartPoint( String inDir, String outDir ) throws Exception
	{
		Configuration conf2 = new Configuration();
		//Job job = new Job( conf, "CountStartPoint" );
		
		// Set input and output paths
		FileSystem fs = FileSystem.get( URI.create(outDir),conf2 );
		Path inputPath = new Path( inDir );  
		Path outputPath = new Path( outDir+"/count" );  
		
		// Delete the output path if it exists
		if( fs.exists(outputPath) )
		{
			if( !fs.delete(outputPath,true) )
			{
				System.out.println( "\n\nDelete output file:"+outDir+" failed!\n\n" );
				return;
			}
		}
		  SparkConf conf = new SparkConf().setAppName("SPCount");
			JavaSparkContext sc = new JavaSparkContext(conf);
	    // Load our input data.
	    JavaRDD<String> input = sc.textFile(inputPath.toString()+"/graph.dat");
	    // Split up into words.
	    JavaRDD<String> graph = input.flatMap(
	      new FlatMapFunction<String, String>() {
	        public Iterator<String> call(String x) {
	          return Arrays.asList(x.split("\t")).iterator();
	        }});
	    // Transform into word and count.
	    JavaPairRDD<String, Integer> CountSp = graph.mapToPair(
	      new PairFunction<String, String, Integer>(){
	        public Tuple2<String, Integer> call(String x){
	          return new Tuple2(x, 1);
	        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
	            public Integer call(Integer x, Integer y){ return x + y;}});
	    // Save the word count back out to a text file, causing evaluation.
	    CountSp.saveAsTextFile(outputPath.toString());
	    sc.close();

	}
	
	// Get all points information into map to record whether a node is visited
	public static void getAllPoints( String dir, Map<Integer,Boolean> map ) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( URI.create(dir),conf );
		Path path = new Path( dir + "/count" );
		FileStatus[] fileStatus = fs.listStatus( path ); 
		
        for( int i = 0; i < fileStatus.length; i++ )
        {  
            if( !fileStatus[i].isDir() ) 
            {  
                FSDataInputStream dis = fs.open( fileStatus[i].getPath() );
                LineReader in = new LineReader( dis,conf );  

                Text line = new Text();
                while( in.readLine(line) > 0 )
                {
                	
        			String[] vStrs = line.toString().replace(")", "").replace("(", "").split(",");
        			
        			map.put( Integer.parseInt(vStrs[0]), false );
        			map.put( Integer.parseInt(vStrs[1]), false );
                }

                dis.close();
                in.close();
            }  
        } 
	}
}


