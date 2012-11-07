package com.simulator.constants;

public class Constants {
	public static final double PI = 3.141592;
	public static final int RAND_MAX = 32767;
	public static final double ARRIVAL_RATE = 1.0;         /* Connections per second */
	public static final int MAX_INT = 2147483647;       /* Maximum positive integer 2^31 - 1 */
	
	// Needed for Request
	public static final int ALL = 0;
	public static final int WEB = 1;
	public static final int P2P = 2;
	public static final int VIDEO = 3;
	public static final int OTHER = 4;	
	public static final int ALL_BUT_WEB = 5;	
	public static final int LINES_SAMPLES = 3730;
}
