package com.simulator.distributions;
public class Request implements Comparable<Request>{
	
	// Private Section
	private int fileId;        // the file id
	private int freq;          // the number of references to be generated for this file
	private int fileSize;      // the file size of this request
	private double prob;                // the prob of referencing this file out of all distinct files
	private int fileType;		//WEB, P2P, VIDEO, OTHER

	// Public Section
	public Request( int initFreq, int initFileSize, int initFileType)
	{
		 freq   = initFreq;
		 fileSize = initFileSize;
		 fileType = initFileType;
	}
	
	// written for the sorting of Requests
	public int compareTo(Request R){
		if(freq > R.freq)
			return 1;
		else if(freq < R.freq)
			return -1;
		else 
			return 0;
	}
	
	public int GetFileId()  // returns the fileid if needed
	{
		return fileId;
	}
	    
	public void SetFileId( int id) //sets the fileid to id
	{
		fileId = id;
	}

	public int  GetFreq()    // returns the frequency if needed
	{
		return freq;
	}

	public void SetFreq(int fr) //sets the freq to f
	{
		freq = fr;
	}
	
	public void SetProb(double probability)
	{
		prob = probability;
	}
	  
	//----------------------------------------------------------------------
	// Request:: GetProb
	//   returns the probability of referencing this file
	//----------------------------------------------------------------------

	public double GetProb()
	{
		return prob;
	}
	
	public int  GetFileSize() //returns file size
	{
		return fileSize;
	}
	
	public void SetFileSize(int fs) //sets the file size
	{
		fileSize = fs;
	}
	   	    
	public int GetFileType()	//returnd the fileType
	{
		return fileType;
	}
	
	public void SetFileType(int ft)
	{
		fileType = ft;
	}

	public void DecFreq()
	{
	   --freq;
	}	
}
