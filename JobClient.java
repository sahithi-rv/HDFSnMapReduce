import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobClient {
	
	 public static String getLookupName(String ip,int port,  String obj){
	 		String s="";
	 		s+="rmi://";
	 		s+=ip;
	 		s+=":";
	 		s+=Integer.toString(port);
	 		s+="/";
	 		s+=obj;
	 		return s;
	 }
	 
	public static void main(String args[]){
		
		mapName = args[0];
		reducerName = args[1];
		input_file = args[2];
		out_file = args[3];
		R = Integer.parseInt(args[4]);
		jobtracker_ip = args[5];
		MapReduce.JobSubmitRequest.Builder job_sub_req = MapReduce.JobSubmitRequest.newBuilder();
		
		job_sub_req.setInputFile(input_file);
		job_sub_req.setMapName(mapName);
		job_sub_req.setNumReduceTasks(R);
		job_sub_req.setOutputFile(out_file);
		job_sub_req.setReducerName(reducerName);
		
		String s = getLookupName(jobtracker_ip, 1099, "JobTrackerImpl");
		try {
			IJobTracker stub = (IJobTracker)Naming.lookup(s);
			byte[] b =stub.jobSubmit(job_sub_req.build().toByteArray());
			MapReduce.JobSubmitResponse job_sub_res = MapReduce.JobSubmitResponse.parseFrom(b);
			System.out.println("job id " + job_sub_res.getJobId());
			
			MapReduce.JobStatusRequest.Builder job_stat_req = MapReduce.JobStatusRequest.newBuilder();
			job_stat_req.setJobId(job_sub_res.getJobId());
			while(true){
				
				try {
					TimeUnit.MILLISECONDS.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
				b = stub.getJobStatus(job_stat_req.build().toByteArray());
				if(b!=null){
					MapReduce.JobStatusResponse job_stat_res = MapReduce.JobStatusResponse.parseFrom(b);
					
					System.out.println("map tasks started " + job_stat_res.getNumMapTasksStarted());
					System.out.println("reduce tasks started " + job_stat_res.getNumReduceTasksStarted());
					
					if (job_stat_res.getJobDone()){
						System.out.println("job done!!");
						break;
					}else{
						System.out.println("stat");
					}
				}
			}
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static String mapName, reducerName, input_file,out_file, jobtracker_ip;
	public static int R;
	
	
}
