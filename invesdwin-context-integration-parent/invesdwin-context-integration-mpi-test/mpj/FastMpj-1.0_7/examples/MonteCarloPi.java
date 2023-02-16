/* MPJ program that uses a Monte Carlo method to compute the value of PI */

package examples;

import mpi.*;
import java.util.Random;

public class MonteCarloPi
{
	private static final double PI = 3.141592653589793238462643;

	public static void main(String args[]) throws Exception 
	{
		long[] hits = new long[1];
		long[] myhits = new long[1];
		double pi,x,y,start,end;
		double[] time = new double[1],total_time = new double[1];
		long niters,myiters;
		int me,size;
	
 	      	args = MPI.Init(args);
		me = MPI.COMM_WORLD.Rank();
		size = MPI.COMM_WORLD.Size();

                if(args.length == 0)
                {
                        if(me==0)
                                System.out.println("Error!. Sintaxis: MonteCarloPi iters");

                        MPI.Finalize();
                        System.exit(0);
                }

                niters = Long.parseLong(args[0]);

		if(me==0)
		{
			System.out.println("Pi calculation using a Monte Carlo method");
			System.out.println("Number of iterations: "+niters);
			System.out.println("Number of processes: "+size);
			System.out.println("Calculating...");
		}

		MPI.COMM_WORLD.Barrier();

		start = MPI.Wtime();

		/* do calculation */ 
        	Random randGenerator = new Random();
		myiters = niters / size;
		myhits[0] = 0;

		for (long i = 0; i < myiters; i++)
		{
            		x = randGenerator.nextDouble();
            		y = randGenerator.nextDouble();
            
			if ((x*x + y*y) <= 1.0)
                		myhits[0]++;
        	}
		/* calculation done */

		MPI.COMM_WORLD.Reduce(myhits, 0, hits, 0, 1, MPI.LONG, MPI.SUM, 0);

		end = MPI.Wtime();

		time[0] = end-start;
		MPI.COMM_WORLD.Reduce(time, 0, total_time, 0, 1, MPI.DOUBLE, MPI.SUM, 0);

	       	if(me==0)
       		{
			pi = 4.0 * ((double) hits[0] / (double) niters);
			System.out.println("DONE!");
       			System.out.format("Avg. Time: %.8f seconds\n",(double)(total_time[0]/size));
       			System.out.format("Pi is approximately %.20f\n",pi);
	       		System.out.format("Error is %.20f\n",Math.abs(pi-MonteCarloPi.PI));
	       	}

		MPI.Finalize();
	}
}
