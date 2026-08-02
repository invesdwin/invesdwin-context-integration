package examples;

import mpi.*;

public class Ring
{
	public static void main(String args[]) throws Exception 
	{
		int me,size;
		int[] passed_num = new int[1];
		passed_num[0]=0;

       		args = MPI.Init(args);
		me = MPI.COMM_WORLD.Rank();
	       	size = MPI.COMM_WORLD.Size();

		System.out.println("my_id "+me+" numprocs "+size);

		if(size > 1)
		{
			if(me == 0)
			{
				passed_num[0]++;

				System.out.println("Root: before sending num="+passed_num[0]+" to dest="+1);

				MPI.COMM_WORLD.Send(passed_num,0,1,MPI.INT,1,0);

				System.out.println("Root: before receiving from source="+(size-1));

				MPI.COMM_WORLD.Recv(passed_num,0,1,MPI.INT,size-1,0);

				System.out.println("Root: after receiving num="+passed_num[0]+" from source="+(size-1));
			}
			else
			{
				System.out.println("Process "+me+": before receiving from source="+(me-1));

				MPI.COMM_WORLD.Recv(passed_num,0,1,MPI.INT,me-1,0);

				System.out.println("Process "+me+": after receiving num="+passed_num[0]+" from source="+(me-1));

				passed_num[0]++;

				System.out.println("Process "+me+": before sending num="+passed_num[0]+" to dest="+((me+1)%size));

				MPI.COMM_WORLD.Send(passed_num,0,1,MPI.INT,(me+1)%size,0);

				System.out.println("Process "+me+": after send to dest="+((me+1)%size));
			}
		}
		else
			System.out.println("numprocs = "+size+", should be run with numprocs > 1");

		MPI.Finalize();
	}
}
