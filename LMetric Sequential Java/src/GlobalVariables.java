
import java.util.ArrayList;

	public class GlobalVariables 
	{
		// Community nodes found by now
		public ArrayList<Integer> commPoints = new ArrayList<Integer>();
		
		// Info of the found community by now 
		double L = 0.0;
		double L_in = 0.0;
		double L_ex = 0.0;
		int inDegree = 0;
		int outDegree = 0;
		
		// Info of the node that is about to be added into the community
		double maxLGain = -1.0;
		double maxL_in = -1.0;
		double maxL_ex = -1.0;
		int maxInDegree = -1;
		int maxOutDegree = -1;
		int maxNode = -1;


		
		// Convert info of the found community by now to a string 
		public String MDegreetoString()
		{
			String MDegree = "" + String.valueOf(L) + " " + String.valueOf(inDegree) + " " +String.valueOf(outDegree);
			return MDegree;
		}
		
		// Convert the all community nodes to a string
		public static String CPtoString( ArrayList<Integer> cp )
		{
			String CPString = "";
			
			for( int i = 0; i < cp.size(); i++ )
			{
				CPString += cp.get(i).toString() + " ";
			}
			
			CPString = CPString.substring( 0, CPString.length()-1 );
			return CPString;
		}
		
		// Convert a string to an array list of community nodes
		public static ArrayList<Integer> StringToCP( String str )
		{
			String[] strings = str.split( " " );
			ArrayList<Integer> cp = new ArrayList<Integer>();
			
			for( int i = 0; i < strings.length; i++ )
			{
				cp.add( Integer.parseInt(strings[i]) );
			}
			
			return cp;
		}
	


}
