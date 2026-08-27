/* MPJ program that computes a matrix-vector multiplication */

package examples;

import mpi.*;

public class MatrixVectorMult
{
	private static boolean DEBUG = false;

	public static void main(String args[]) throws Exception 
	{
		int me,size,n,i,j,count,remainder,myRows,aux = 0;
		double[] matrix = null,local_matrix = null,vector = null,result = null,local_result = null;
		int[] sendcounts,senddispls,recvcounts,recvdispls;
		double[] seconds_comp = new double[1],seconds_comms = new double[1],total_seconds_comp = new double[1],total_seconds_comms = new double[1];
		double start,end;

 	      	args = MPI.Init(args);
		me = MPI.COMM_WORLD.Rank();
		size = MPI.COMM_WORLD.Size();

		if(args.length == 0)
		{
			if(me==0)
				System.out.println("Error!. Sintaxis: MatrixVectorMult N (NxN matrix)");

			MPI.Finalize();
			System.exit(0);
		}

		n = Integer.parseInt(args[0]);
		vector = new double[n];
		sendcounts = new int[size];
                senddispls = new int[size];
                recvcounts = new int[size];
                recvdispls = new int[size];

		if(me==0)
		{
			System.out.println("Matrix-Vector multiplication");
			System.out.println("Square matrix size: "+n+"x"+n);
                        System.out.println("Number of processes: "+size);
			matrix = new double[n*n];
			result = new double[n];

			/* Initialize Matrix and Vector */
			for(i=0;i<n;i++)
			{
				vector[i] = i;

				for(j=0;j<n;j++)
					matrix[i*n+j] = i+j;
        		}

			if (DEBUG)
			{
				System.out.println("Matrix is:");
				for(i=0;i<n;i++)
				{
					for(j=0;j<n;j++)
						System.out.print(matrix[i*n+j]+" ");
	
					System.out.println();
				}
				System.out.println("Vector is:");

				for(i=0;i<n;i++)
					System.out.print(vector[i]+" ");
				System.out.println();
			}
		}

		seconds_comp[0] = 0.0;
		seconds_comms[0] = 0.0;
		count = n/size;
		remainder = n%size;
		myRows = me < remainder ? count + 1 : count;
                local_result = new double[myRows];
                local_matrix = new double[myRows*n];

		for (i = 0; i < size; ++i)
                {
                	recvcounts[i] = (i < remainder) ? count + 1 : count;
                        sendcounts[i] = recvcounts[i] * n;
                        recvdispls[i] = aux;
                        senddispls[i] = aux * n;
                        aux += recvcounts[i];

			if(DEBUG && me == 0)
				System.out.println("rank "+i+": rows "+recvcounts[i]+" sendcounts "+sendcounts[i]+" senddispls "+senddispls[i]+" recvcounts "+recvcounts[i]+" recvdispls "+recvdispls[i]);
                }

		MPI.COMM_WORLD.Barrier();

		start = MPI.Wtime();
		MPI.COMM_WORLD.Bcast(vector,0,n,MPI.DOUBLE,0);
		MPI.COMM_WORLD.Scatterv(matrix, 0, sendcounts, senddispls, MPI.DOUBLE, local_matrix, 0, myRows*n, MPI.DOUBLE, 0);
		end = MPI.Wtime();

		seconds_comms[0] += end-start;

		/* Computation */
		start = MPI.Wtime();
		for(i=0;i<myRows;i++)
		{
			local_result[i]=0;
			for(j=0;j<n;j++)
				local_result[i] += local_matrix[i*n+j]*vector[j];
		}
		end = MPI.Wtime();

		seconds_comp[0] += end-start;

		start = MPI.Wtime();
		MPI.COMM_WORLD.Gatherv(local_result, 0, myRows, MPI.DOUBLE, result, 0, recvcounts, recvdispls, MPI.DOUBLE, 0);
		end = MPI.Wtime();

		seconds_comms[0] += end-start;

		MPI.COMM_WORLD.Reduce(seconds_comp, 0, total_seconds_comp, 0, 1, MPI.DOUBLE, MPI.SUM, 0);
		MPI.COMM_WORLD.Reduce(seconds_comms, 0, total_seconds_comms, 0, 1, MPI.DOUBLE, MPI.SUM, 0);

	       	if(me==0)
       		{
			if (DEBUG)
                        {
				System.out.println("Result is:");

        	                for(i=0;i<n;i++)
	        	                System.out.print(result[i]+" ");
                        	System.out.println();
			}

			total_seconds_comp[0] = total_seconds_comp[0] / size;
			total_seconds_comms[0] = total_seconds_comms[0] / size;
			double total = total_seconds_comp[0] + total_seconds_comms[0];
       			System.out.format("Avg. Computation Time: %.8f seconds\n",total_seconds_comp[0]);
       			System.out.format("Avg. Communication Time: %.8f seconds\n",total_seconds_comms[0]);
			long flops = (long)2*n*n;
			System.out.format("MFlops: %.4f\n",(double)(flops/total)/1E6);
	       	}
		MPI.Finalize();
	}
}
