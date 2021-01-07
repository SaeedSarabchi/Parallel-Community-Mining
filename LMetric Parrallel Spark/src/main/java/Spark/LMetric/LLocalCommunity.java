package Spark.LMetric;


import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import Spark.LMetric.GlobalVariables;

import org.apache.hadoop.fs.FSDataInputStream;

public class LLocalCommunity 
{
	public static void findLLocalCommunity( int startPoint, String inDir, String outDir, 
			int index, GlobalVariables gv, Map<Integer,Boolean> map ) throws Exception
	{
		
		System.out.println( "\n Hello findMLocalCommunity \n" );
		
		gv.commPoints = new ArrayList<Integer>();
		gv.commPoints.add( startPoint );
		//Update Community Border
		updateCommunityBorders(inDir, gv);
		
		//testBorders:
		//for( Map.Entry<Integer,ArrayList<Integer>> entry: gv.Borders.entrySet() )
		//{
			//System.out.println("Border for Node:"+ entry.getKey() + "With the value Count:"+ entry.getValue().size());
		//}
		
		gv.L = 0.0;
		gv.inDegree = 0;
		gv.inDegree = 0;
		// Get the out degree of the start point
		gv.outDegree = getOutDegree( outDir, startPoint );
		gv.L_in = 0.0;
		gv.L_ex = 0.0;
		
		//System.out.println( "\n gv.outDegree = "+Integer.toString(gv.outDegree)+" \n" );
		
		if( gv.outDegree == -1 )
			return;
		
		// Keep finding nodes until no more nodes or maxMGain is less than current M
		while( true )
		{
			// Compute Lin and Lex and LGain
			computeL( inDir, outDir, gv, index);
			// Get the node that yields the max M
			getMaxL( outDir, gv, index, map );
			
			if( gv.maxLGain <= gv.L || gv.maxNode == -1 )
			{
				System.out.println( "\n  getMaxM Break!!!\n");
				break;
			}
			
			// Update community infomation
			gv.L = gv.maxLGain;
			gv.L_in =  gv.maxL_in ;
			gv.L_ex =  gv.maxL_ex ;
			gv.inDegree +=  2*gv.maxInDegree;
			gv.outDegree = gv.outDegree - gv.maxInDegree + gv.maxOutDegree;
			gv.commPoints.add(gv.maxNode );
			
			//Update Community Border
			updateCommunityBorders(inDir, gv);
			
			//testBorders:
			//for( Map.Entry<Integer,ArrayList<Integer>> entry: gv.Borders.entrySet() )
			//{
				//System.out.println("Border for Node:"+ entry.getKey() + "With the value Count:"+ entry.getValue().size());
			//}
			
			System.out.println( "Maximum Node Found: "+gv.maxNode + "\t" + gv.maxLGain + "\t" + gv.maxL_in + "\t" + gv.maxL_ex );
			System.out.println( "Community Points So Far for index "+index+" : "+GlobalVariables.CPtoString(gv.commPoints) );
		}
		
		 ArrayList<Tuple2<Integer,Integer>> communityGraph =  new  ArrayList<Tuple2<Integer,Integer>> ();
		communityGraph = extractCommunityGraph(inDir, gv);
		
		ArrayList<Integer> prunedNodes = communityExamination(communityGraph, gv);
		System.out.println("Pruned Nodes Are: "+prunedNodes.toString());
		if(!prunedNodes.contains(startPoint))
		{
			for(Integer node:prunedNodes)
			{
				gv.commPoints.remove(node);
			}
			
			//update L Metrics 
			double[] updatedMetrics = evaluateLMetrics(communityGraph,gv);
			
			gv.L = updatedMetrics[0];
			gv.L_in = updatedMetrics[1];
			gv.L_ex = updatedMetrics[2];
			gv.inDegree = (int) updatedMetrics[3];
			gv.outDegree = (int) updatedMetrics[4];
			updateCommunityBorders(inDir, gv);
			
			
			String localCommunity = "Detected 	Community: "+GlobalVariables.CPtoString(gv.commPoints);
			System.out.println( "Into File: " + localCommunity );
			
			
			// Write result to index.rst
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get( URI.create(outDir),conf );
			FSDataOutputStream out = fs.create( new Path(outDir+"/"+index+".rst") );
			out.writeUTF( localCommunity );
			 out.close();
		}
		else
		{
			gv.commPoints.clear();;
			gv.L = 0.0;
			gv.inDegree = 0;
			gv.outDegree = 0;
			gv.L_in = 0.0;
			gv.L_ex = 0.0;
			gv.Borders.clear();
		}
		
       
	}
	
	
	private static  ArrayList<Tuple2<Integer,Integer>> extractCommunityGraph(String inDir, GlobalVariables gv) {
		
		// Set input path
				Path inputPath = new Path( inDir );  

			    SparkConf conf = new SparkConf().setAppName("updateCommunityBorder");
				JavaSparkContext sc = new JavaSparkContext(conf);

				
				//broadcast the Global Variables
					final Broadcast< GlobalVariables> broadcastedGV = sc.broadcast(gv);
						
						
				// Load our input data.
				//Assuming that our input file is named graph.dat
			    JavaRDD<String> input = sc.textFile(inputPath.toString()+"/graph.dat");
			    // Split up into words.
			    JavaRDD<String> CommunityMappedGraph = input.flatMap(
			      new FlatMapFunction<String, String>() {
			        public Iterator<String> call(String x) 
			        {

			        	//retrieving Global Variables
			        	GlobalVariables gv = broadcastedGV.getValue();
			        	
			        	Text myKey = new Text();
			            Text myValue = new Text();
			            List<String> MappedResults = new ArrayList<String>();
			        	String[] lines = x.split("\n");
			        	for(Iterator<String> i = Arrays.asList(lines).iterator(); i.hasNext(); ) 
			        	{
			        	    String item = i.next();
			        	    String[] strings = item.split( "\t" );
			        	    int left = Integer.parseInt( strings[0] );
			    			int right = Integer.parseInt( strings[1] );
			    			
			    			boolean containL = gv.commPoints.contains(left);
			    			boolean containR = gv.commPoints.contains(right);
			            	
			                if( containL|| containR )
		                	{
		                	MappedResults.add((left+","+ right));
		                	}
			                
			    			
			        	}
			        	
			        	return MappedResults.iterator();
			        }
			        
						
			        });
			    
			    
			    //Creating Community Graph
			    gv.Borders.clear();
			    List<String> CommunityGraph = CommunityMappedGraph.collect();
			    
			    //CommunityGraphEdges
			    ArrayList<Tuple2<Integer,Integer>> graphEdges = new  ArrayList<Tuple2<Integer,Integer>>(); 
			    
			    for (String edge : CommunityGraph) {
			    	String[] values =edge.split(",");
			    	int left = Integer.parseInt(values[0]);
			    	int right = Integer.parseInt(values[1]);
			    	
			    	graphEdges.add(new Tuple2<Integer, Integer>(left, right));
				}
			    
			    
			    sc.close();
			
		
		
		
		return graphEdges;
	}

	private static ArrayList<Integer> communityExamination( ArrayList<Tuple2<Integer,Integer>>  communityGraph, GlobalVariables gv) 
	{
		ArrayList<Integer> prunedNodes = new ArrayList<Integer>();
		
		// remove node_id from cluster
		// and compute Lp_in and Lp_out
		// compare with the L_in and L_out
		// keep only if in the third case in paper
		ArrayList<Integer> originalCommunityNodes = new ArrayList<Integer>(gv.commPoints);
		double oldL_ext =gv.L_ex; 
		for(Integer communityNode:originalCommunityNodes)
		{
			gv.commPoints.remove(communityNode);
			double[] new_LMetrics = evaluateLMetrics(communityGraph, gv);
			if(!(new_LMetrics[2]>=oldL_ext))
			{
				prunedNodes.add(communityNode);
				System.out.println("Pruned: "+communityNode+" LMetrics: "+new_LMetrics[0]+","+new_LMetrics[1]+","+new_LMetrics[2]);
			}
			gv.commPoints.add(communityNode);
		}
		return prunedNodes;
	}
	
	private static double[] evaluateLMetrics( ArrayList<Tuple2<Integer,Integer>> communityGraph, GlobalVariables gv) 
{
	double L_in = 0.0;
	double L_ex = 0.0;
	double L = 0.0;
	int inDegree=0;
	int outDegree=0;
	HashMap<Integer, Boolean> borders=  new HashMap<Integer, Boolean>();
	
	for( Tuple2<Integer,Integer> entry: communityGraph)
	{
		int node1 = entry._1;
		int node2 = entry._2;
    	
		if ( gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
    	{
			outDegree +=1;
    		borders.put(node1,true);
    		
    	}
    	else if ( !gv.commPoints.contains( node1 ) && gv.commPoints.contains( node2 ) )
    	{
    		outDegree +=1;
    		borders.put(node2,true);
    	}
    	else if( gv.commPoints.contains( node1 ) && gv.commPoints.contains( node2 ) )
    	{
    		inDegree +=2;
    	}
			
	}
	
	if(gv.commPoints.size()>0)
		L_in= (double)(inDegree)/gv.commPoints.size();
	
	//in case border is NULL
	L= L_in;
	
	if(borders.size()!=0)
	{
		L_ex= (double) (outDegree) /  borders.size();
		L = (double)(L_in)/L_ex;
	}
	
	
	double[] results = {L,L_in,L_ex, inDegree, outDegree };
	return results;
}

	// Compute Lin and Lex and LGain
	public static void computeL( String inDir, String outDir, GlobalVariables gv, int index ) throws Exception
	{
		Configuration conf2 = new Configuration();
	    
		
		// Set input and output paths
		FileSystem fs = FileSystem.get( URI.create(outDir),conf2 );
		Path inputPath = new Path( inDir );  
		Path outputPath = new Path( outDir+"/"+index );  
		if( fs.exists(outputPath) )
		{
			if( !fs.delete(outputPath,true) )
			{
				System.out.println( "\n\nDelete output file:"+outDir+" failed!\n\n" );
				return;
			}
		}
		
		SparkConf conf = new SparkConf().setAppName("ComputeL");
		//conf.set( "CPString", GlobalVariables.CPtoString(gv.commPoints) );
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		//broadcast the Global Variables
		final Broadcast< GlobalVariables> broadcastedGV = sc.broadcast(gv);
		
		
		
    // Load our input data.
	//Assuming that our input file is named graph.dat
    JavaRDD<String> input = sc.textFile(inputPath.toString()+"/graph.dat");
    // Split up into words.
    JavaRDD<String> graph = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterator<String> call(String x) {

        	//retrieving Global Variables
        	GlobalVariables gv = broadcastedGV.getValue();
        	
        	Text myKey = new Text();
            Text myValue = new Text();
            List<String> MappedResults = new ArrayList<String>();
        	String[] lines = x.split("\n");
        	for(Iterator<String> i = Arrays.asList(lines).iterator(); i.hasNext(); ) {
        	    String item = i.next();
        	    String[] strings = item.split( "\t" );
        	    int left = Integer.parseInt( strings[0] );
    			int right = Integer.parseInt( strings[1] );
    			
    			boolean containL = gv.commPoints.contains(left);
    			boolean containR = gv.commPoints.contains(right);
    			
    			
    			
    			// Number indicates which position is community nodes
    			// We do not process if both of the nodes are community nodes or it starts with a community node
    			
    			// If only one edge between two nodes
    			if( containL && !containR )
    			{
    				myKey.set( strings[1] );
    				myValue.set( strings[0]+"\t1" );
    				MappedResults.add(myKey+","+ myValue);

    			}
    			else if( !containL && containR )
    			{
    				myKey.set( strings[0] );
    				myValue.set( strings[1]+"\t1" );
    				MappedResults.add(myKey+","+ myValue);
    			}
    			else if( !containL && !containR )
    			{
    				myKey.set( strings[0] );
    				myValue.set( strings[1]+"\t-1" );
    				MappedResults.add(myKey+","+ myValue);
    				
    				myKey.set( strings[1] );
    				myValue.set( strings[0]+"\t-1" );
    				MappedResults.add(myKey+","+ myValue);
    			}    
        	}
        	
        	return MappedResults.iterator();
			
        }});
    
    // reduce phase
    JavaPairRDD<String, String> ComputeM1 = graph.mapToPair(
      new PairFunction<String, String, String>(){
        public Tuple2<String, String> call(String x){
        	String[] strings = x.split( "," );
			return new Tuple2(strings[0], strings[1]);
			
        }}).reduceByKey(new Function2<String, String, String>(){
            public String call(String x, String y){  return x+","+y; }});
    		
    JavaPairRDD<String,String> ComputeM2 = ComputeM1.mapToPair(
    		new PairFunction<Tuple2<String, String>, String, String>() {

				public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
					// TODO Auto-generated method stub
					
					//retrieving Global Variables
		        	GlobalVariables gv = broadcastedGV.getValue();
					
					
	        		
	        		int in = 0;
	        		int out = 0;
	        		
	        		Text myValue = new Text();
	        		
	        		String[] values = t._2.split(",");
	        		for( String text: values)
	        		{
	        			String[] vStrs = text.toString().split( "\t" );
	        			if( vStrs.length == 2 )
	        			{
	        				int cpPosition = Integer.parseInt(vStrs[1]);

	        				switch( cpPosition )
	        				{
	        					case 1:
	        						in++;
	        						break;
	        					case -1:
	        						out++;
	        						break;
	        					default:
	        						break;
	        				}
	        			}
	        		}
	        		
	        		if( in != 0 )
	        		{
	        			
	        			double myL_inGain = (double)(gv.inDegree+2*in) /  (gv.commPoints.size()+1);
	        			double myL_exGain = 0;
	    				double myLGain = myL_inGain;
	        			
	    				int borderSize=gv.Borders.size();
	    				
	    				
	    				//If Node is in border itself
	    				if(out>0)
	    					borderSize += 1;
	    				
	    				//If the inKey results in some Community node to change from border to core
	    				for( Map.Entry<Integer,ArrayList<Integer>> borderEntry: gv.Borders.entrySet() )
	    				{
	    					int borderKey = borderEntry.getKey();
	    					ArrayList<Integer> borderValues = borderEntry.getValue();
	    					
	    					if (borderValues.size()==1)
	    						if (borderValues.get(0) == Integer.parseInt(t._1))
	    							borderSize-=1;
	    					
	    				}
	    				
	    				
	    				if(borderSize!=0)
	    				{
	    					//Just For testing!!!
	    					//borderSize=gv.commPoints.size()+1;
	    					//Testing code Finished
	    					
	    					myL_exGain= (double)(gv.outDegree-in+out) /  borderSize;
	    					myLGain= (double)(myL_inGain)/myL_exGain;
	    				}

	        			
	        			
	        			//Only output when myLGain > L and 
	        			if( myLGain > gv.L )
	        			{
	        				String LGainInfo = String.valueOf(myLGain) + "\t" + String.valueOf(myL_inGain) + "\t" + String.valueOf(myL_exGain)+ "\t" + String.valueOf(in) + "\t" + String.valueOf(out); 
	        				myValue.set( LGainInfo);
	        				return new Tuple2<String, String>(t._1, myValue.toString());
	        			}
	        		}
	          
	        		
					return new Tuple2<String, String>(null, myValue.toString());
				}
    			
    		}).filter(new Function<Tuple2<String,String>, Boolean>() {
				
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					return v1._1 != null;
				}
			});

    // Save the Computed L measures back out to a text file, causing evaluation.
    ComputeM2.saveAsTextFile( generateFileName(outDir + "\t" + String.valueOf(index)));
    sc.close();
    
	}
	
	public static String generateFileName( String dirInfo )
	{
		String[] infos = dirInfo.split("\t");
		return infos[0] + "/" + infos[1] + "/" + "LInfo";
	}
	
	// Get the node that yields the max M
	public static void getMaxL( String dir, GlobalVariables gv, int index, Map<Integer,Boolean> map ) throws Exception
	{
		double myLGain;
		double myL_In;
		int key;
		
		double maxLGain = -1.0;
		int maxNode = -1;
		int maxInDegree = -1;
		int maxOutDegree = -1;
		double maxL_in = -1.0;
		double maxL_ex = -1.0;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( URI.create(dir),conf );
		Path path = new Path( dir + "/" + index + "/LInfo/" );
		FileStatus[] fileStatus = fs.listStatus( path ); 
		
		System.out.println( "\n  getMaxL: File Status Length: "+fileStatus.length+ "\n");
		
        for( int i = 0; i < fileStatus.length; i++ )
        {  
        	String fileName = fileStatus[i].getPath().getName();
            //if( !fileStatus[i].isDir() && fileName.startsWith("MInfo") ) 
        	if( !fileStatus[i].isDir() )
            {  
                FSDataInputStream dis = fs.open( fileStatus[i].getPath() );
                LineReader in = new LineReader( dis,conf );  
                System.out.println( "\n  Goes1 \n");

                Text line = new Text();
                while( in.readLine(line) > 0 )
                {
                	 System.out.println( "\n  Goes2 \n");
        			String[] key_Value = line.toString().replace(")", "").replace("(", "").split(",");
        			//System.out.println( "\n  key_Value[0] "+key_Value[0]+" \n");
        			if(!key_Value[0].equals("null"))
        			{
	        			String[] values = key_Value[1].split("\t");
	        			String[] vStrs={key_Value[0],values[0], values[1], values[2],values[3], values[4]};
	        			//System.out.println( "\n  MInfo Keys: "+key_Value[0]+" , "+values[0]+" , "+values[1] +" , "+ values[2]+" \n");
	        			myLGain = Double.parseDouble( vStrs[1] );
	        			myL_In = Double.parseDouble( vStrs[2] );
	        			key = Integer.parseInt(vStrs[0]);
	        			if( myLGain >= maxLGain && myL_In>gv.L_in && map.get(Integer.parseInt(vStrs[0])) == false )
	        			{
	        				//Breaking Ties to lower NodeIDs
	    					if(myLGain == maxLGain && maxNode !=-1)
	    						if (key>maxNode )
	    							continue;
	    					maxLGain = myLGain;
	    					maxNode = key;
	    					maxL_in = myL_In;
	    					maxL_ex = Double.parseDouble(vStrs[3]);
	    					maxInDegree =Integer.parseInt(vStrs[4]);
	    					maxOutDegree = Integer.parseInt(vStrs[5]);
	        			}
	                }
                }

                dis.close();
                in.close();
            }  
        }
		
		gv.maxLGain = maxLGain;
		gv.maxNode = maxNode;
		gv.maxInDegree = maxInDegree;
		gv.maxOutDegree = maxOutDegree;
		gv.maxL_in = maxL_in;
		gv.maxL_ex = maxL_ex;
	}
	
	// Get the out degree of the start point
	public static int getOutDegree( String dir, int startPoint ) throws Exception
	{
		int count = -1;
		String spStr = String.valueOf(startPoint);
		boolean find = false;
		
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
        			
        			//System.out.println( "\n Umad \n");
        			//System.out.println( "\n vStrs[0] = "+vStrs[0]+"\n");
        			if( vStrs[0].equals(spStr) )
        			{
        				count = Integer.parseInt(vStrs[1]);
        				find = true;
        				break;
        			}
                }

                dis.close();
                in.close();
            }  
            
            if( find )
            	break;
        } 
        return count;
	}
	private static void updateCommunityBorders(String inDir, GlobalVariables gv) 
	{
		// Set input path
		Path inputPath = new Path( inDir );  

	    SparkConf conf = new SparkConf().setAppName("updateCommunityBorder");
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		//broadcast the Global Variables
			final Broadcast< GlobalVariables> broadcastedGV = sc.broadcast(gv);
				
				
		// Load our input data.
		//Assuming that our input file is named graph.dat
	    JavaRDD<String> input = sc.textFile(inputPath.toString()+"/graph.dat");
	    // Split up into words.
	    JavaRDD<String> BordersMap = input.flatMap(
	      new FlatMapFunction<String, String>() {
	        public Iterator<String> call(String x) {

	        	//retrieving Global Variables
	        	GlobalVariables gv = broadcastedGV.getValue();
	        	
	        	Text myKey = new Text();
	            Text myValue = new Text();
	            List<String> MappedResults = new ArrayList<String>();
	        	String[] lines = x.split("\n");
	        	for(Iterator<String> i = Arrays.asList(lines).iterator(); i.hasNext(); ) {
	        	    String item = i.next();
	        	    String[] strings = item.split( "\t" );
	        	    int left = Integer.parseInt( strings[0] );
	    			int right = Integer.parseInt( strings[1] );
	    			
	    			boolean containL = gv.commPoints.contains(left);
	    			boolean containR = gv.commPoints.contains(right);
	    			
	    			
	    			// If only one edge between two nodes
	    			if( containL && !containR )
	    			{
	    				myKey.set( strings[0] );
	    				myValue.set(strings[1]);
	    				MappedResults.add(myKey+","+ myValue);
	    			}
	    			else if( !containL && containR )
	    			{
	    				myKey.set( strings[1] );
	    				myValue.set( strings[0] );
	    				MappedResults.add(myKey+","+ myValue);
	    			}
	    			
	        	}
	        	
	        	return MappedResults.iterator();
				
	        }});
	    
	    // reduce phase
	    JavaPairRDD<String, String> BordersReduced = BordersMap.mapToPair(
	      new PairFunction<String, String, String>(){
	        public Tuple2<String, String> call(String x){
	        	String[] strings = x.split( "," );
				return new Tuple2(strings[0], strings[1]);
				
	        }}).reduceByKey(new Function2<String, String, String>(){
	            public String call(String x, String y){  return x+","+y; }});
	    
	    //Updating Community Borders
	    gv.Borders.clear();
	    List<Tuple2<String, String>> BordersCollection = BordersReduced.collect();
	    for (Tuple2<String, String> border : BordersCollection) {
	    	String[] values = border._2.split(",");
	    	
	    	ArrayList<Integer> borderEdges = new ArrayList<Integer>(); 
    		for( String text: values)
    		{
    			borderEdges.add(Integer.parseInt(text));
    		}
	    	gv.Borders.put(Integer.parseInt(border._1), borderEdges);
		}
	    
	    
	    sc.close();
	}
}
