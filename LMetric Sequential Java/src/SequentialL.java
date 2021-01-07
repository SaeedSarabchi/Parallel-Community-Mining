
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

//import GlobalVariables;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;

public class SequentialL {
	public void findLCommunitySeqLocally( int startNode, String inDir, String outDir )
	{
		Map<Integer,Boolean> visitedRecords = new HashMap<Integer,Boolean>(); 
		Map<Integer,Integer> allOutDegrees = new HashMap<Integer,Integer>(); 
		GlobalVariables gv = new GlobalVariables();
		
		// Compute all nodes' outdegrees and initialize all visited records to false
		// FileHandling
		try
		{
            
			FileInputStream fis = new FileInputStream( inDir );
			InputStreamReader isr = new InputStreamReader(fis);
			LineNumberReader lnr = new LineNumberReader(isr);
			
			String line = null;
            while ( ( line = lnr.readLine() ) != null ) 
            {
            	String[] edge = line.split("\t");
            	int node1 = Integer.parseInt(edge[0]);
            	int node2 = Integer.parseInt(edge[1]);
            	
            	visitedRecords.put( node1, false );
            	visitedRecords.put( node2, false );
            	
        		int count1 = allOutDegrees.containsKey( node1 ) ? allOutDegrees.get(node1) : 0;
        		allOutDegrees.put( node1, count1+1 );
        		
        		int count2 = allOutDegrees.containsKey( node2 ) ? allOutDegrees.get(node2) : 0;
        		allOutDegrees.put( node2, count2+1 );
            }
            
            lnr.close();
            isr.close();
            fis.close();
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		
		visitedRecords.put(startNode, true);
		
		
		//index on Remaining Nodes
		int indexRemaining=0;
		
		for( int i = 0; ; i++ )
		{
			boolean over = true;
			
			// Find local community for a particular start point
			findLocalCommunity( startNode, inDir, gv, allOutDegrees, visitedRecords );
			
			//If No Community Found, Make the visited feature of startNode false.
			if(gv.commPoints.size()==0)
				visitedRecords.put(startNode, false);
			
			// Set the status of all community nodes to visited
			for( int j = 0; j < gv.commPoints.size(); j++ )
			{
				int node = gv.commPoints.get(j);
				visitedRecords.put( node, true );
			}
			
			// Write results to a file
			// FileHandling
			if( gv.commPoints.size()>0)
			{
				try
		        {
		            File wf = new File( outDir );
		            BufferedWriter wfbw = new BufferedWriter( new FileWriter(wf, true) );
		            
		            wfbw.write( GlobalVariables.CPtoString( gv.commPoints ) );
		        	wfbw.newLine();
		            
		            wfbw.close();
		            wfbw = null;
		            wf = null;
			    }
		        catch (Exception e) 
		        {
		        	e.printStackTrace();
			    }
			}
			
			// If there is still node that has not been visited, set it to start point
			ArrayList<Integer> al = new ArrayList<Integer>();
			for( Map.Entry<Integer,Boolean> entry: visitedRecords.entrySet() )
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
				
				startNode = al.get(indexRemaining);
				visitedRecords.put( startNode, true );
				over = false;
			}

			// If no more nodes to visit
			if( over == true )	
				break;
		}
	}
	

	public void findLocalCommunity( int startNode, String inDir, GlobalVariables gv, Map<Integer,Integer> allOutDegrees, Map<Integer,Boolean> visitedRecords )
	{
		gv.commPoints = new ArrayList<Integer>();
		gv.commPoints.add( startNode );
		
		gv.L = 0.0;
		gv.inDegree = 0;
		gv.outDegree = allOutDegrees.get( startNode );
		gv.L_in = 0.0;
		gv.L_ex = 0.0;
		
		
		//boolean breakFlag= false;
		while( true )
		{
			Map<Integer,String> potentialNodes = new HashMap<Integer,String>(); 
			
			// Compute L_in and L_ex and LGain
			computeL( inDir, gv, potentialNodes );
			
				// Get the node that yields the max L
				getMaxL( gv, potentialNodes, visitedRecords );
				
				// If the nodes do not contribute to LGain, then break
				if( gv.maxLGain <= gv.L || gv.maxNode == -1 )
				{
					break;
				}
				
			// Update community infomation
			gv.L = gv.maxLGain;
			gv.L_in =  gv.maxL_in ;
			gv.L_ex =  gv.maxL_ex ;
			gv.inDegree +=  2*gv.maxInDegree;
			gv.outDegree = gv.outDegree - gv.maxInDegree + gv.maxOutDegree;
			gv.commPoints.add(gv.maxNode );
			
			System.out.println( "Maximum Node Found: "+gv.maxNode + "\t" + gv.maxLGain + "\t" + gv.maxL_in + "\t" + gv.maxL_ex );
			System.out.println( "Community Points So Far:  "+GlobalVariables.CPtoString(gv.commPoints) );
			
			
		}
		
		ArrayList<Map.Entry<Integer,Integer>> communityGraph =  new ArrayList<Map.Entry<Integer,Integer>> ();
		communityGraph = extractCommunityGraph(inDir, gv);
		
		ArrayList<Integer> prunedNodes = communityExamination(communityGraph, gv);
		System.out.println("Pruned Nodes Are: "+prunedNodes.toString());
		if(!prunedNodes.contains(startNode))
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
			
			
			String localCommunity = "Detected 	Community: "+GlobalVariables.CPtoString(gv.commPoints);
			System.out.println( "Into File: " + localCommunity );
		}
		else
		{
			gv.commPoints.clear();;
			gv.L = 0.0;
			gv.inDegree = 0;
			gv.outDegree = 0;
			gv.L_in = 0.0;
			gv.L_ex = 0.0;
		}
	}
	
	private ArrayList<Map.Entry<Integer,Integer>> extractCommunityGraph(String inDir, GlobalVariables gv) {
		
		ArrayList<Map.Entry<Integer,Integer>>  communityGraph =  new ArrayList<Map.Entry<Integer,Integer>> ();
		
		// Extracting the Inner Graph in the Community
		try
		{
			FileInputStream fis = new FileInputStream( inDir );
			InputStreamReader isr = new InputStreamReader(fis);
			LineNumberReader lnr = new LineNumberReader(isr);
			
			String line = null;
            while ( ( line = lnr.readLine() ) != null ) 
            {
            	String[] edge = line.split("\t");
            	int node1 = Integer.parseInt(edge[0]);
            	int node2 = Integer.parseInt(edge[1]);
            	
            if( gv.commPoints.contains( node1 ) || gv.commPoints.contains( node2 ) )
            	{
            		communityGraph.add(new AbstractMap.SimpleEntry(node1, node2));
            	}
            }
            
            lnr.close();
            isr.close();
            fis.close();
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		return communityGraph;
	}

	private ArrayList<Integer> communityExamination(ArrayList<Map.Entry<Integer,Integer>>  communityGraph, GlobalVariables gv) 
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
	
	private double[] evaluateLMetrics(ArrayList<Map.Entry<Integer,Integer>> communityGraph, GlobalVariables gv) 
{
	double L_in = 0.0;
	double L_ex = 0.0;
	double L = 0.0;
	int inDegree=0;
	int outDegree=0;
	HashMap<Integer, Boolean> borders=  new HashMap<Integer, Boolean>();
	
	for( Map.Entry<Integer,Integer> entry: communityGraph)
	{
		int node1 = entry.getKey();
		int node2 = entry.getValue();
    	
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

	public void computeL( String inDir, GlobalVariables gv, Map<Integer,String> potentialNodes )
	{
		// FileHandling
		HashMap<Integer, Integer> inDegrees =  new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> outDegrees =  new HashMap<Integer, Integer>();
		HashMap<Integer, String> borders =  new HashMap<Integer, String>();
		
		try
		{
			FileInputStream fis = new FileInputStream( inDir );
			InputStreamReader isr = new InputStreamReader(fis);
			LineNumberReader lnr = new LineNumberReader(isr);
			
			String line = null;
            while ( ( line = lnr.readLine() ) != null ) 
            {
            	String[] edge = line.split("\t");
            	int node1 = Integer.parseInt(edge[0]);
            	int node2 = Integer.parseInt(edge[1]);
            	int count = 0;
            	
            	
            	if ( gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
            	{
            		count = inDegrees.containsKey( node2 ) ? inDegrees.get(node2) : 0;
            		inDegrees.put( node2, count+1 );
            		
            		if(borders.get(node1)==null)
            			borders.put(node1,String.valueOf(node2)+"\t" );
            		else
            			borders.put(node1,borders.get(node1)+String.valueOf(node2)+"\t" );
            	}
            	else if ( !gv.commPoints.contains( node1 ) && gv.commPoints.contains( node2 ) )
            	{
            		count = inDegrees.containsKey( node1 ) ? inDegrees.get(node1) : 0;
            		inDegrees.put( node1, count+1 );
            		
            		if(borders.get(node2)==null)
            			borders.put(node2,String.valueOf(node1)+"\t" );
            		else
            			borders.put(node2, borders.get(node2)+String.valueOf(node1)+"\t");
            	}
            	else if( !gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
            	{
            		count = outDegrees.containsKey( node1 ) ? outDegrees.get(node1) : 0;
            		outDegrees.put( node1, count+1 );
            		count = outDegrees.containsKey( node2 ) ? outDegrees.get(node2) : 0;
            		outDegrees.put( node2, count+1 );
            	}
            }
            
            lnr.close();
            isr.close();
            fis.close();
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		
		
		for( Map.Entry<Integer,Integer> entry: inDegrees.entrySet() )
		{
			int inKey = entry.getKey();
			int inValue = entry.getValue();
			
			if( inValue != 0 )
			{
				
				int outValue = outDegrees.containsKey(inKey) ? outDegrees.get(inKey) : 0;
				//Populating LMetric measures
				double myL_inGain = (double)(gv.inDegree+2*inValue) /  (gv.commPoints.size()+1);
				double myL_exGain = 0;
				double myLGain = myL_inGain;
				

				int borderSize=borders.size();
				
				
				//If Node is in border itself
				if(outValue>0)
					borderSize += 1;
				//If the inKey results in some Community node to change from border to core
				for( Map.Entry<Integer,String> borderEntry: borders.entrySet() )
				{
					int borderKey = borderEntry.getKey();
					String borderValues = borderEntry.getValue();
					String[] borderNeighbors = borderValues.split("\t");
					if (borderNeighbors.length==1)
						if (Integer.parseInt(borderNeighbors[0])==inKey)
							borderSize-=1;
					
				}
				
				if(borderSize!=0)
				{
					myL_exGain= (double)(gv.outDegree-inValue+outValue) /  borderSize;
					myLGain= (double)(myL_inGain)/myL_exGain;
				}
				if ( myLGain > gv.L )
				{
					String LGainInfo = String.valueOf(myLGain) + "\t" + String.valueOf(myL_inGain) + "\t" + String.valueOf(myL_exGain)+ "\t" + String.valueOf(inValue) + "\t" + String.valueOf(outValue);
					potentialNodes.put(inKey, LGainInfo);
				}
			}
		}
	}
	
	
	public void getMaxL( GlobalVariables gv, Map<Integer,String> potentialNodes, Map<Integer,Boolean> visitedRecords )
	{
		double myLGain;
		double myL_In;
		
		double maxLGain = -1.0;
		int maxNode = -1;
		int maxInDegree = -1;
		int maxOutDegree = -1;
		double maxL_in = -1.0;
		double maxL_ex = -1.0;
		
		for( Map.Entry<Integer,String> potentialNode: potentialNodes.entrySet() )
		{
			int key = potentialNode.getKey();
			String value = potentialNode.getValue();
			
			if( visitedRecords.get(key) == false )
			{
				String[] vStrs = value.toString().split("\t");
				myLGain = Double.parseDouble( vStrs[0] );
				myL_In = Double.parseDouble(vStrs[1]);
				
				//case 1 and 3 in the paper
				if( (myLGain >= maxLGain) && (myL_In>gv.L_in)  )
				{
					//Breaking Ties to lower NodeIDs
					if(myLGain == maxLGain && maxNode !=-1)
						if (key>maxNode )
							continue;
					maxLGain = myLGain;
					maxNode = key;
					maxL_in = myL_In;
					maxL_ex = Double.parseDouble(vStrs[2]);
					maxInDegree =Integer.parseInt(vStrs[3]);
					maxOutDegree = Integer.parseInt(vStrs[4]);
				}
			}
		}
		
		gv.maxLGain = maxLGain;
		gv.maxNode = maxNode;
		gv.maxInDegree = maxInDegree;
		gv.maxOutDegree = maxOutDegree;
		gv.maxL_in = maxL_in;
		gv.maxL_ex = maxL_ex;
	}
	



}
