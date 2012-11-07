package com.simulator.distributions;

import com.simulator.constants.Constants;

public class Distributions {
	
	/* 
	 * Generate a random floating point number uniformly distributed in [0,1] 
	 */
	public double Uniform01(){
		double randnum;

		/* get a random positive integer from random() */
		randnum = (double) 1.0 * Math.random();
		return( randnum );
	}

	/* 
	 * Generate a random floating point number from an exponential
	 * distribution with mean mu.                                     
	 */
	public double Exponential(double mu){
	    double randnum, ans;
	    randnum = Uniform01();
	    ans = -(mu) * Math.log(randnum);
	    return( ans );
	}

	/* 
	 * Generate a random integer number uniformly distributed in [0,range) 
	 */
	public int UniformInt(int range)
	{	
	    int randnum;
	    randnum = (int)(Math.random()*range);
	    return randnum;
	}

	/*
	 * Taking uniformly distributed in [0, Tlife) random "time" values
	 * Based on this value we generate the expected arrival rate
	 * according to the exponential decay rule.
	 *  See JSAC'07 paper by Guo et al. for more information.  
	 */
	public double exponentialDecayArrivalRate(double lamda, double tau, double tracesSeeding)
	{
		return lamda*Math.exp(-UniformInt((int)(tau*Math.log(lamda*tracesSeeding))) / tau );
	}

	/*
	 * Weibull distribution
	 */
	public double Weibull(int rank, double weibullK, double weibullL)
	{		
		return ((weibullK / weibullL) * (Math.pow((rank/weibullL),(weibullK-1))*(Math.exp(Math.pow((rank/weibullL), (weibullK))*-1))));
	}


	/*
	 * Returns a random value for the "life span" of a request 
	 * following the Pareto distribution
	 * as described by W. Tang et al. (Computer Networks 51 (2007) 336-356)
	 */
	public double ParetoCDF(float alpha)
	{
		return 1 / Math.pow(1- Uniform01(), 1/alpha);
	}

	/*
	 * Returns a random value for the "life span" of a request 
	 * following the Lognormal distribution
	 * as described by W. Tang et al. (Computer Networks 51 (2007) 336-356)
	 */
	public double Lognormal(float mean, float std)
	{
		double lognormMeanSqr = Math.pow(mean, 2);
	    double lognormVariance= Math.pow(std, 2);
	
	    double paramMean = Math.log( lognormMeanSqr / Math.sqrt(lognormVariance + lognormMeanSqr));
	    double paramStd  = Math.sqrt(Math.log((lognormVariance + lognormMeanSqr) / lognormMeanSqr));
	    
	    return Math.exp(paramMean + paramStd * StandardNormal());
	}

	/*
	 * Marsaglia polar method for generating a random number following
	 * the standard normal distribution
	 */
	public double StandardNormal()
	{
	    double u1, u2, v1=0, v2=0, w, y, x1; 	//params of the normal variate
	    double normalVariate; 					//the lognormal variate
			
		w = 2.0;
		while (w > 1)
		{
			 u1 = Uniform01();
			 u2 = Uniform01();
			 v1 = 2*u1 - 1;
			 v2 = 2*u2 - 1;
			 w = v1*v1 + v2*v2;
		}
	
		y  = Math.pow((-2*Math.log(w)/w), 0.5);
		x1 = v1*y;
		normalVariate = x1;		  
		return normalVariate;
	}
	
	/* 
	 * Returns a random number following the normal distribution
	 * with the given parameters.
	 */
	public double Normal(float mean, float std)
	{
		return mean+std*StandardNormal();
	}
	
	public double non_standard_value(double value, float mean, float std)
	{
		return value*std + mean;
	}
	
	public double standard_normal_quantile(double p)
	{
		return Math.sqrt(2)*inverse_erf(2*p-1);
	}
	
	/*
	 * http://en.wikipedia.org/wiki/Error_function#Inverse_function
	 */ 
	public double inverse_erf(double x)
	{
		double a = 0.140012;
		return sgn(x)*Math.sqrt(Math.sqrt(Math.pow(((2/Constants.PI*a) + Math.log(1-Math.pow(x,2))/2),2)
				- ( Math.log(1-Math.pow(x,2)) /a) ) - ( ((2/Constants.PI*a) + Math.log(1-Math.pow(x,2))/2) ) );
	}
	
	public double sgn(double x)
	{
		if (x > 0) return 1;
		if (x < 0) return -1;
		return 0;
	}
}
