package com.simulator.distributions;
import java.io.*;
import java.io.IOException;
import java.io.RandomAccessFile;

//This class (Node) is just for miscelaneous use
public class Node{  //store a value and its cummulative probability
	public int value;
	public double cummProb;

	// count of the values
	/*
	 * CDF
	 * 
	 * Produce a cummulative distribution values from the input
	 * list of integers, which is already in ascending order. inList is
	 * input array, outIndex is the number of items in the resulting CDF
	 * values
	 */

	@SuppressWarnings("unused")
	public Node[] CDF(int inList[], NoofElement nele, int noofDistinctDocs)
	{
		if (inList == null)
		{
			System.out.println("The list passed to CDF is null");
			System.exit(1);
		}

		Node outList[] = new Node[noofDistinctDocs]; //each node in this new list will
		//contain each unique element and its cumulative probability

		if (outList == null)
		{
			System.out.println("Error allocation memory in CDF\n");
			System.exit(1);
		}

		long currVal, nextVal;

		currVal 	= inList[0];	//current value is the first value in the list
		int outIndex 	= 0;	       //index of where unique values will go in outList
		int inIndex;

		for (inIndex = 1; inIndex < noofDistinctDocs; inIndex++)
		{
			nextVal = inList[inIndex];
	            
			if (nextVal != currVal) //we have come to the end of prev unique item
			{				
				outList[outIndex] = new Node();
				outList[outIndex].value = (int)currVal;
				outList[outIndex].cummProb = (double)(inIndex)/noofDistinctDocs;
				outIndex++;
				currVal = nextVal; //start a new set of values
			}
		}

		// The last item in the list must be
		outList[outIndex] = new Node();
		outList[outIndex].value 	= (int)currVal;
		outList[outIndex].cummProb 	= (double)(inIndex)/noofDistinctDocs;
		outIndex++;   //this is the total no items in outList;

		// Now copy the exact no of items into a new list and return it
		Node newOutList[] = new Node[outIndex];

		if(newOutList == null)
		{
			System.out.println("Error allocating memory in CDF\n");
			System.exit(1);
		}

		// Copy outList to newOutList before returning the pointer to it
		for (int i=0; i < outIndex; i++)        
	            newOutList[i] = outList[i];
		
		nele.number = outIndex;
		return newOutList;
	}
	
	/*
	 * OutputPopAndFileSize
	 * 
	 * This outputs the info about the distinct files in the workload into
	 * a file. The info has only 2 column. The first column is the popularity
	 * of each distinct file arranged in descending order. This could easily
	 * be extracted and used to plot the popularity ranking graph. The second
	 * column is the file size of each distinct file in the workload, but is
	 * not sorted. You may have to sort it when you extract that column to
	 * plot CDF or LLCD stuffs.
	 */
	void OutputPopAndFileSize(String statisticsFile, int noofDistinctDocs, Request uniqueDoc[], boolean append)
	{
		
		String br = "\r\n";
		
		 try {
			 FileOutputStream file = new FileOutputStream(statisticsFile, append); 
			 DataOutputStream out   = new DataOutputStream(file); 
			 
			 for (int i=0; i<noofDistinctDocs; i++){
				
				 out.writeBytes(Integer.toString(uniqueDoc[i].GetFileId()) + "\t"); 
				 out.writeBytes(Integer.toString(uniqueDoc[i].GetFreq()) + "\t");  
				 out.writeBytes(Long.toString((long)uniqueDoc[i].GetFileSize()) + "\t");  
				 out.writeBytes(Integer.toString(uniqueDoc[i].GetFileType()) );
				 out.write(br.getBytes());
				 //out.flush(); 
					 
			 }
			 out.close(); 		
			 
			 /*File file = new File(statisticsFile);
	         RandomAccessFile raf = new RandomAccessFile(file, fmode);   
	         for (int i=0; i<noofDistinctDocs; i++){
	        	 raf.writeChars(Integer.toString(uniqueDoc[i].GetFileId()) + "\t");
	        	 //raf.writeBytes("\t");
	        	 raf.writeChars(Integer.toString(uniqueDoc[i].GetFreq()) + "\t");
	        	 //raf.writeBytes("\t");
	        	 raf.writeChars(Integer.toString(uniqueDoc[i].GetFileSize()) + "\t");
	        	 //raf.writeBytes("\t");
	        	 raf.writeChars(Integer.toString(uniqueDoc[i].GetFileType()));
	        	 raf.writeChars("\n");
	         }*/
		 }
		 catch (IOException e) {
	            System.out.println("IOException:");
	            e.printStackTrace();
	        }
	}


	int FindValue(Node[] a, int n, double item)
	{
	//n is the no of items in this list

	int l 		= 0;
	int r 		= n-1;
	boolean done 	= false;
	int loc;
	loc = 0;
		
	while (!done)
	{
		int mid = (l+r)/2;

		if ((item <= a[mid].cummProb && mid == 0) ||
			(item <= a[mid].cummProb && item > a[mid-1].cummProb))
		{
			loc  = mid;
			done = true;
		}
		else if (item > a[mid].cummProb && ((mid +1) == r))
		{
			loc  = mid+1;
			done = true;
		}
		else if (item  > a[mid].cummProb)
			l = mid;
		else
			r = mid;
	}

	if ((item == a[loc].cummProb) ||  (loc == 0) )
		return a[loc].value;

	//else interpolate
	int value = (int) Math.ceil((((item - a[loc-1].cummProb)*(a[loc].value -
	a[loc-1].value)) / (a[loc].cummProb - a[loc-1].cummProb)) + a[loc-1].value);

	return value;
	}

}

	class NoofElement{
	int number;
	}


