/* MPJ program that computes a matrix-matrix multiplication */

package examples;

import mpi.*;

public class MatrixMatrixMult
{
	private static boolean DEBUG = false;

	public static void main(String args[]) throws Exception 
	{
		int me,size,n,i,j,k,count,remainder,myRows,rows;
		double[] matrix_A = null,matrix_B = null,matrix_C = null,local_matrix_A = null,local_matrix_C = null;
		int[] sendcounts,senddispls,recvcounts,recvdispls;
		double[] seconds_comp = new double[1],seconds_comms = new double[1],total_seconds_comp = new double[1],total_seconds_comms = new double[1];
		double start,end;

 	      	args = MPI.Init(args);
		me = MPI.COMM_WORLD.Rank();
		size = MPI.COMM_WORLD.Size();

		if(args.length == 0)
		{
			if(me==0)
				System.out.println("Error!. Sintaxis: MatrixMatrixMult N (NxN matrix)");

			MPI.Finalize();
			System.exit(0);
		}

		n = Integer.parseInt(args[0]);
		matrix_B = new double[n*n];
		sendcounts = new int[size];
                senddispls = new int[size];
                recvcounts = new int[size];
                recvdispls = new int[size];

		if(me==0)
		{
			System.out.println("Matrix-Matrix multiplication");
			System.out.println("Square matrix size: "+n+"x"+n);
			System.out.println("Number of processes: "+size);
			matrix_A = new double[n*n];
			matrix_C = new double[n*n];

			/* Initialize Matrix */
			for(i=0;i<n;i++)
			{
				for(j=0;j<n;j++)
				{
					matrix_A[i*n+j] = i+j;
					matrix_B[i*n+j] = i+j;
				}
        		}

			if (DEBUG)
			{
				System.out.println("Matrix A is:");
				for(i=0;i<n;i++)
				{
					for(j=0;j<n;j++)
						System.out.print(matrix_A[i*n+j]+" ");
	
					System.out.println();
				}
				System.out.println("Matrix B is:");
                                for(i=0;i<n;i++)
                                {
                                        for(j=0;j<n;j++)
                                                System.out.print(matrix_B[i*n+j]+" ");

                                        System.out.println();
                                }

			}
		}

		seconds_comp[0] = 0.0;
		seconds_comms[0] = 0.0;
		count = n/size;
		remainder = n%size;
		myRows = me < remainder ? count + 1 : count;
                local_matrix_C = new double[myRows*n];
                local_matrix_A = new double[myRows*n];

		for (i = 0; i < size; ++i)
                {
			rows = (i < remainder) ? count + 1 : count;
                	recvcounts[i] = rows * n;
                        sendcounts[i] = rows * n;
			if(i>0)
			{
                        	recvdispls[i] = recvdispls[i-1]+recvcounts[i-1];
	                        senddispls[i] = senddispls[i-1]+sendcounts[i-1];
			}
			else
			{
				recvdispls[i] = 0;
                                senddispls[i] = 0;
			}
			if(DEBUG && me == 0)
                                System.out.println("rank "+i+": rows "+rows+" sendcounts "+sendcounts[i]+" senddispls "+senddispls[i]+" recvcounts "+recvcounts[i]+" recvdispls "+recvdispls[i]);
                }

		MPI.COMM_WORLD.Barrier();

		start = MPI.Wtime();
		MPI.COMM_WORLD.Bcast(matrix_B,0,n*n,MPI.DOUBLE,0);
		MPI.COMM_WORLD.Scatterv(matrix_A, 0, sendcounts, senddispls, MPI.DOUBLE, local_matrix_A, 0, myRows*n, MPI.DOUBLE, 0);
		end = MPI.Wtime();

		seconds_comms[0] += end-start;

		/* Computation */
		start = MPI.Wtime();
		for (i=0; i<myRows; i++)
		{
        		for (j=0; j<n; j++)
			{
            			for (k=0; k<n; k++)
			                local_matrix_C[i*n+j] += local_matrix_A[i*n+k] * matrix_B[k*n+j];
        		}
    		}
		end = MPI.Wtime();

		seconds_comp[0] += end-start;

		start = MPI.Wtime();
		MPI.COMM_WORLD.Gatherv(local_matrix_C, 0, myRows*n, MPI.DOUBLE, matrix_C, 0, recvcounts, recvdispls, MPI.DOUBLE, 0);
		end = MPI.Wtime();

		seconds_comms[0] += end-start;

		MPI.COMM_WORLD.Reduce(seconds_comp, 0, total_seconds_comp, 0, 1, MPI.DOUBLE, MPI.SUM, 0);
		MPI.COMM_WORLD.Reduce(seconds_comms, 0, total_seconds_comms, 0, 1, MPI.DOUBLE, MPI.SUM, 0);

	       	if(me==0)
       		{
			if (DEBUG)
                        {
				System.out.println("Matrix result is:");

				for(i=0;i<n;i++)
                                {
                                        for(j=0;j<n;j++)
                                                System.out.print(matrix_C[i*n+j]+" ");

                                        System.out.println();
                                }
			}

			total_seconds_comp[0] = total_seconds_comp[0] / size;
			total_seconds_comms[0] = total_seconds_comms[0] / size;
			double total = total_seconds_comp[0] + total_seconds_comms[0];
       			System.out.format("Avg. Computation Time: %.8f seconds\n",total_seconds_comp[0]);
       			System.out.format("Avg. Communication Time: %.8f seconds\n",total_seconds_comms[0]);
			long flops = (long)2*n*n*n;
			System.out.format("MFlops: %.4f\n",(double)(flops/total)/1E6);
	       	}
		MPI.Finalize();
	}
}
