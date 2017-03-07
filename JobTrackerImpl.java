import java.awt.List;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.omg.CORBA.Request;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTrackerImpl implements IJobTracker{
	
	public static String getLookupName(String ip, String obj){
		String s="";
		s+="rmi://";
		s+=ip;
		s+="/";
		s+=obj;
		return s;
	}
	
	public static int getNumMapTasksComp( ){
		
		int cnt = 0;
		for( MapReduce.MapTaskStatus ms : map_status[Job_id]){
			if( (ms!=null) && ms.getTaskCompleted() ){
				cnt++;
			}
		}
		return cnt;
	}
	
	public static int getNumRedTasksComp( ){
		
		int cnt = 0;
		for( MapReduce.ReduceTaskStatus rs : reduce_status[Job_id] ){
			if( (rs!=null) && rs.getTaskCompleted() ){
				cnt++;
			}
		}
		return cnt;
	}

	// get the block locations corresponding to the input file
	public static java.util.List<Hdfs.BlockLocations> getBlocLocations(String file_name){
		
		//open file 
		Hdfs.OpenFileRequest.Builder open_file_req = Hdfs.OpenFileRequest.newBuilder();
		open_file_req.setFileName(file_name);
		open_file_req.setForRead(true);
		//byte[] b = NameNodeImpl.openFile(open_file_req.build().toByteArray());
		 
		String s = getLookupName(namenode_ip, "NameNodeImpl");
		try {
			INameNode stub = (INameNode)Naming.lookup(s);
			byte[] b = stub.openFile(open_file_req.build().toByteArray());
			Hdfs.OpenFileResponse open_file_res = Hdfs.OpenFileResponse.parseFrom(b);
			java.util.List<Integer> block_nums =  open_file_res.getBlockNumsList();
			
			Hdfs.BlockLocationRequest.Builder block_loc_req = Hdfs.BlockLocationRequest.newBuilder();
			block_loc_req.addAllBlockNums(block_nums);
			b = stub.getBlockLocations(block_loc_req.build().toByteArray());
			Hdfs.BlockLocationResponse block_loc_res = Hdfs.BlockLocationResponse.parseFrom(b);
			java.util.List<Hdfs.BlockLocations> hdfs_block_locs = block_loc_res.getBlockLocationsList();
			return hdfs_block_locs;
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
		
		
	}
	
	// converts Hdfs.BlockLocations to MapReduce.BlockLocations
	//block_locs list is set;
	public static void hdfs2MapRBlockLocs(java.util.List<Hdfs.BlockLocations> hdfs_blockLocs){
	 
		MapReduce.BlockLocations.Builder mapR_bloc = MapReduce.BlockLocations.newBuilder();
		MapReduce.DataNodeLocation.Builder mapR_dn_loc = MapReduce.DataNodeLocation.newBuilder();
		try{
			for(Hdfs.BlockLocations bloc_loc : hdfs_blockLocs){
				
				mapR_bloc.setBlockNumber(bloc_loc.getBlockNumber());
				java.util.List<Hdfs.DataNodeLocation> dn_loc = bloc_loc.getLocationsList();
				for( Hdfs.DataNodeLocation loc : dn_loc ){
					mapR_dn_loc.setIp(loc.getIp());
					mapR_dn_loc.setPort(loc.getPort());
					mapR_bloc.addLocations(mapR_dn_loc);
				}
				block_locs.add(mapR_bloc.build());
			}
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	public byte[] jobSubmit(byte[] inp)  throws RemoteException{
		try{
			MapReduce.JobSubmitRequest job_submit_req = MapReduce.JobSubmitRequest.parseFrom(inp);
			MapReduce.JobSubmitResponse.Builder job_submit_res = MapReduce.JobSubmitResponse.newBuilder();

			num_R.add(job_submit_req.getNumReduceTasks()) ;
			Job_id++;
			job_queue.add(job_submit_req);
			
			if(job_submit_req.hasInputFile()){
				MapName.add(job_submit_req.getMapName());
				ReduceName.add(job_submit_req.getReducerName());
				
				// setting block locations from input file
				java.util.List<Hdfs.BlockLocations> hdfs_block_locs = getBlocLocations(job_submit_req.getInputFile());
				hdfs2MapRBlockLocs(hdfs_block_locs);
				num_M.add(hdfs_block_locs.size());
			
				//update map queue and map info list
				MapReduce.MapTaskInfo.Builder map_task_info = MapReduce.MapTaskInfo.newBuilder();
				int task_id=1;
				 Queue<MapReduce.MapTaskInfo> mq = new LinkedList<MapReduce.MapTaskInfo>() ;
				 Queue<Integer> rq = new LinkedList<Integer>();
				
				 java.util.List<MapReduce.MapTaskInfo> mti  = new ArrayList<MapReduce.MapTaskInfo>();;
				//mti.clear();
				if( (block_locs!=null) &&   !( block_locs.isEmpty()) ){
					for(MapReduce.BlockLocations bloc_loc : block_locs){
						
						map_task_info.clearInputBlocks();
						map_task_info.setJobId(Job_id);
					//	System.out.println("tid "+task_id );
						map_task_info.setTaskId(task_id);
						map_task_info.setMapName(MapName.get(0));
						map_task_info.addInputBlocks(bloc_loc);
						mti.add(map_task_info.build());
						mq.add(map_task_info.build());
						
						task_id++;
					}
					map_task_infoList.add(mti);
					map_queue.add(mq);
					//System.out.println("MQ SZ .................." + mq.size());
					//System.out.println("MQ SZ .................." + map_queue.get(0).size());
				}
				// update reduce queue
				
				if( (num_R!=null) &&  !(num_R.isEmpty()) ){
					for(int i = 1;i<= num_R.get(0);i++){
						rq.add(i);
					}
				}
				reduce_queue.add(rq);
			}else{
				System.out.println("No input filename provided");
			}
			
			/*MapReduce.MapTaskStatus.Builder ms = MapReduce.MapTaskStatus.newBuilder();
			//MapReduce.ReduceTaskStatus.Builder rs = MapReduce.ReduceTaskStatus.newBuilder();
			ms.setJobId(0);
			ms.setTaskId(0);
			ms.setMapOutputFile("");
			ms.setTaskCompleted(false);
			map_status.add(ms.build());*/
			/*rs.setJobId(0);
			rs.setTaskCompleted(false);
			rs.setTaskId(0);
			reduce_status.add(rs.build());*/
			// building the job_submit_response
			job_submit_res.setJobId(Job_id);
			job_submit_res.setStatus(1);
			
			return job_submit_res.build().toByteArray();
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static void updateJob(){
		job_queue.remove();
		num_M.remove(0);
		num_R.remove(0);
		MapName.remove(0);
		ReduceName.remove(0);
		map_queue.remove(0);
		reduce_queue.remove(0);
	/*	map_status.clear();
		MapReduce.MapTaskStatus.Builder ms = MapReduce.MapTaskStatus.newBuilder();
		
		ms.setJobId(0);
		ms.setTaskId(0);
		ms.setMapOutputFile("");
		ms.setTaskCompleted(false);
		map_status.add(ms.build());*/
		//reduce_status.clear();
		map_task_infoList.remove(0);
		
	}
	
	@Override
	public byte[] getJobStatus(byte[] inp)  throws RemoteException{
		try{
				//System.out.println("block locs sz: "+ map_queue.size());
				//if( map_queue.size()!=0)
				//System.out.println("map queue sz: " + map_queue.get(0).size());
			//if(!(num_M.isEmpty()) && !(map_queue.isEmpty())){
				MapReduce.JobStatusResponse.Builder job_status_res = MapReduce.JobStatusResponse.newBuilder();
				if(num_M!=null && !(num_M.isEmpty()) ){
					job_status_res.setTotalMapTasks(num_M.get(0));
					//System.out.println("num_M " + num_M.get(0));
				}else{
					job_status_res.setTotalMapTasks(0);
				}
				if( num_M!=null && !(num_M.isEmpty())  && !(map_queue==null)  && !(map_queue.isEmpty())){
					job_status_res.setNumMapTasksStarted(num_M.get(0) - map_queue.get(0).size());
					//System.out.println("mq " + map_queue.get(0).size());
				}else{
					job_status_res.setNumMapTasksStarted(0);
				}
				if(num_R!=null && !(num_R.isEmpty()) ){
					job_status_res.setTotalReduceTasks(num_R.get(0));
				}else{
					job_status_res.setTotalReduceTasks(0);
				}
				if( num_R!=null && !(num_R.isEmpty())  && !(reduce_queue==null)  && !(reduce_queue.isEmpty())){
					job_status_res.setNumReduceTasksStarted(num_R.get(0) - reduce_queue.get(0).size());
				}else{
					job_status_res.setNumReduceTasksStarted(0);
				}
				
				
				
				//job done
				if( (num_M!=null) && !(num_M.isEmpty()) && num_M.get(0) == getNumMapTasksComp() ){
					reduce_phase.add(true);
					num_map_tasks_assigned = 0;
					if( num_R.get(0) == getNumRedTasksComp() ){
						job_status_res.setJobDone(true);
					//	updateJob();
					}
					else
						job_status_res.setJobDone(false);
				}else{
					job_status_res.setJobDone(false);
				}
				
				return job_status_res.build().toByteArray();
		//	}
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException{
		InputStream in = new ByteArrayInputStream(inp);
		MapReduce.HeartBeatResponse.Builder heart_beat_res = MapReduce.HeartBeatResponse.newBuilder();
		int tt_id, task_id, job_id, no_free_map_slots, no_free_reduce_slots;
		
		java.util.List<MapReduce.MapTaskStatus> map_task_status;
		java.util.List<MapReduce.ReduceTaskStatus> reduce_task_status;
		
		try {
			//extracting request attributes
			MapReduce.HeartBeatRequest heart_beat_req;
			
			heart_beat_req = MapReduce.HeartBeatRequest.parseFrom(in);
			
			tt_id = heart_beat_req.getTaskTrackerId();
			
		//	System.out.println("b4 req");
			no_free_map_slots = heart_beat_req.getNumMapSlotsFree();
			no_free_reduce_slots = heart_beat_req.getNumReduceSlotsFree();
			map_task_status =  heart_beat_req.getMapStatusList();
			reduce_task_status = heart_beat_req.getReduceStatusList();
		
			//System.out.println("after req");
			if(flag == 0){
				System.out.println("first hb");
				flag =1;
			}
			//map task status
			if((map_task_status!=null) && !(map_task_status.isEmpty())){
				for( MapReduce.MapTaskStatus map_status_entry : map_task_status) {
					task_id = map_status_entry.getTaskId();
					job_id = map_status_entry.getJobId();
					map_status[Job_id][task_id] = map_status_entry;
					if(task_id > num_M.get(0)){
						System.out.println(":o "+ task_id);
					}
				}
				//System.out.println("map stat sz: " + map_status.size());
			}
			
			// reduce task status
			if((reduce_task_status!=null) && !(reduce_task_status.isEmpty())){
				for( MapReduce.ReduceTaskStatus reduce_status_entry : reduce_task_status) {
					task_id = reduce_status_entry.getTaskId();
					job_id = reduce_status_entry.getJobId();
					reduce_status[Job_id][task_id] = reduce_status_entry;
				}
				/*System.out.println("red stat sz: " + reduce_status.size());*/
			}
			MapReduce.MapTaskInfo map_task_info ;
			MapReduce.ReducerTaskInfo.Builder reduce_task_info = MapReduce.ReducerTaskInfo.newBuilder();
			// map _response
			
			if( (map_queue!=null) && !(map_queue.isEmpty()) && !( map_queue.get(0).isEmpty())){
				for( int i = 1;(i<=no_free_map_slots) && ( !map_queue.get(0).isEmpty() ); i++ ){
					System.out.println("free mq sz: " + map_queue.get(0).size());
					map_task_info = map_queue.get(0).remove();
					/*map_task_info.setTaskId(task_id);
					map_task_info.setMapName(MapName);
					map_task_info.addInputBlocks(block_locs.remove());*/
					heart_beat_res.addMapTasks(map_task_info);
				}
				System.out.println("map tasks hb: " + heart_beat_res.getMapTasksCount());
			}
			if( map_queue.get(0).size()!=0 )
				System.out.println("HB MQ SZ: " + map_queue.get(0).size());
			if( (reduce_queue!=null) && !(reduce_phase.isEmpty()) &&  reduce_phase.get(0) && num_map_tasks_assigned<num_M.get(0) ){
				MapReduce.MapTaskStatus ms;
				//System.out.println("ms");
				System.out.println("RQ SZ: " + reduce_queue.size());
				System.out.println("rq free: " + no_free_reduce_slots);
				if((reduce_queue.get(0).isEmpty())){
					System.out.println("reduce q empty");
				}
				for( int i = 1;!(reduce_queue.isEmpty()) && !(reduce_queue.get(0).isEmpty()) && i<= no_free_reduce_slots;i++ ){
					task_id = reduce_queue.get(0).remove();
					reduce_task_info.setTaskId(task_id);
					reduce_task_info.setReducerName(ReduceName.get(0));
					/*String map_output = map_status.get(task_id).getMapOutputFile();
					reduce_task_info.setOutputFile(map_output);*/
					num_MR = num_M.get(0)/num_R.get(0);
					for(int j = 1;j<=num_MR;j++){
						//if((map_status!=null) && !(map_status.isEmpty()) ){
						num_map_tasks_assigned++;
						ms = map_status[Job_id][num_map_tasks_assigned];
						if(ms!=null){
							String map_output = ms.getMapOutputFile();
							reduce_task_info.addMapOutputFiles(map_output);
						}else{
							num_map_tasks_assigned--;
						}
					
					}
					System.out.println(reduce_task_info.getMapOutputFilesCount() + " files for " + task_id);
					
					//shud set ouput file name
					heart_beat_res.addReduceTasks(reduce_task_info);
					
				}
				System.out.println("reduce tasks hb: " + heart_beat_res.getReduceTasksCount());
			}
			//System.out.println("return");
			return heart_beat_res.build().toByteArray();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		} catch(NullPointerException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	public static void main(String args[]) throws RemoteException{
		namenode_ip = args[0];
		TTs = Integer.parseInt(args[1]);
		jobtracker_ip = args[2];
		jt_port = Integer.parseInt(args[3]);
		try {
			JobTrackerImpl obj = new JobTrackerImpl();
			IJobTracker req_skel = (IJobTracker) UnicastRemoteObject.exportObject(obj, jt_port);
			Naming.rebind("JobTrackerImpl", req_skel);
		} catch (RemoteException e) {
			
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// class variables
	public static int flag = 0;	
	public static Integer Job_id=0;
	public static java.util.List<String> MapName = new ArrayList<String>();
	public static java.util.List<String> ReduceName = new ArrayList<String>();
	public static String jobtracker_ip ;
	public static Integer TTs;
	public static int jt_port;
	public static java.util.List<Boolean> reduce_phase  = new ArrayList<Boolean>() ;
	public static String namenode_ip;
	public static java.util.List<Integer> num_R = new ArrayList<Integer>();  // number of reduce tasks for jobs
	public static java.util.List<Integer> num_M = new ArrayList<Integer>() ;
	public static Integer num_MR = 2 ;
	public static Queue<MapReduce.JobSubmitRequest> job_queue  = new LinkedList<MapReduce.JobSubmitRequest>() ;
	public static java.util.List< Queue<MapReduce.MapTaskInfo> > map_queue = new ArrayList<Queue<MapReduce.MapTaskInfo>>();
	public static java.util.List< Queue<Integer> > reduce_queue  = new ArrayList<Queue<Integer>>();
	public static java.util.List<MapReduce.BlockLocations> block_locs = new ArrayList<MapReduce.BlockLocations>();
	//public static List<MapReduce.BlockLocations> mapR_block_locs = new List<MapReduce.BlockLocations>(); 
	public static java.util.List< java.util.List<MapReduce.MapTaskInfo>  > map_task_infoList = new ArrayList<java.util.List<MapReduce.MapTaskInfo> >();; // contains blocknumber and taskid for the corresponding maptas
	public static int num_map_tasks_assigned = 0;
	public static MapReduce.MapTaskStatus[][] map_status = new MapReduce.MapTaskStatus[5][10000];
	public static MapReduce.ReduceTaskStatus[][] reduce_status = new MapReduce.ReduceTaskStatus[5][10000];

	
	//public static MapReduce.MapTaskStatus[] m = new MapReduce.MapTaskStatus[10000];
	
}