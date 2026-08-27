/* MPJ program that uses numerical integration to compute the value of PI */

package examples;

import mpi.*;

public class NumericalIntegrationPi
{
	private static final double PI = 3.141592653589793238462643;

	public static void main(String args[]) throws Exception 
	{
		double[] mypi = new double[1];
		double[] pi = new double[1];
		long n;
		double h,x,sum,start,end;
		double[] time = new double[1],total_time = new double[1];
		int me,size;
	
 	      	args = MPI.Init(args);
		me = MPI.COMM_WORLD.Rank();
		size = MPI.COMM_WORLD.Size();

                if(args.length == 0)
                {
                        if(me==0)
                                System.out.println("Error!. Sintaxis: NumericalIntegrationPi iters");

                        MPI.Finalize();
                        System.exit(0);
                }

                n = Long.parseLong(args[0]);

		if(me==0)
		{
			System.out.println("Pi calculation using numerical integration");
			System.out.println("Number of intervals: "+n);
			System.out.println("Number of processes: "+size);
			System.out.println("Calculating...");
		}

		MPI.COMM_WORLD.Barrier();

		start = MPI.Wtime();

		/* do calculation */
		h   = 1.0 / (double) n; 
		sum = 0.0; 

		for (long i = me + 1; i <= n; i += size) 
		{
			x = h * ((double)i - 0.5); 
			sum += (4.0 / (1.0 + x*x));
		} 
		mypi[0] = h * sum; 
		/* calculation done */

		MPI.COMM_WORLD.Reduce(mypi, 0, pi, 0, 1, MPI.DOUBLE, MPI.SUM, 0);

		end = MPI.Wtime();

		time[0] = end-start;
                MPI.COMM_WORLD.Reduce(time, 0, total_time, 0, 1, MPI.DOUBLE, MPI.SUM, 0);

	       	if(me==0)
       		{
			System.out.println("DONE!");
       			System.out.format("Avg. Time: %8f seconds\n",(double)(total_time[0]/size));
       			System.out.format("Pi is approximately %.20f\n",pi[0]);
	       		System.out.format("Error is %.20f\n",Math.abs(pi[0]-NumericalIntegrationPi.PI));
	       	}

		MPI.Finalize();
	}
}
