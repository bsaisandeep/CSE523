package com.simulator.ccn;

public class ObjectListEntry {
			
		private int ObjectId;
		private int ObjectSize;

		ObjectListEntry () {
			
			ObjectId = 0;
			ObjectSize = 0;		
		}
		
		public ObjectListEntry (int tempObjectId, int tempObjectSize) {
			
			ObjectId = tempObjectId;
			ObjectSize = tempObjectSize;
		}
		
		
		public void setObjectId (int temp) {
			ObjectId = temp;
		}
		
		public void setObjectSize (int temp) {
			ObjectSize = temp;
		}
		
		public int getObjectId () {
			return ObjectId;
		}
		
		public int getObjectSize () {
			return ObjectSize;
		}
	}

