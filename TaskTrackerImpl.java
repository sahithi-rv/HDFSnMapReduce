import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit; 


public class TaskTrackerImpl {
	
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
	
	public static String getMapOutputFile(int task_id, int job_id){
		String s = "map_out_file-";
		s+=job_id;
		s+="-";
		s+= task_id;
		s+="-";
		s+=TT_id;
		return s;
	}
	
	public static void main(String args[]) throws InterruptedException,  RemoteException, NotBoundException{
		jobtracker_ip = args[0];
		TT_id = Integer.parseInt(args[1]);
		namenode_ip = args[2];
		MapReduce.HeartBeatRequest.Builder heart_beat_req = MapReduce.HeartBeatRequest.newBuilder();
		
	    ThreadFactory threadFactory = Executors.defaultThreadFactory();
	    //creating the ThreadPoolExecutor
		ThreadPoolExecutor executorPool = new ThreadPoolExecutor(nThreads, nThreads, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(nThreads), threadFactory);
		//start the monitoring thread
		/*MonitorThread monitor = new MonitorThread(executorPool, 3);
		Thread monitorThread = new Thread(monitor);
		monitorThread.start();*/
		MapReduce.MapTaskInfo map_task_info ;
		MapReduce.MapTaskStatus.Builder map_task_status = MapReduce.MapTaskStatus.newBuilder();
		MapReduce.ReducerTaskInfo reduce_task_info ;
		MapReduce.ReduceTaskStatus.Builder reduce_task_status = MapReduce.ReduceTaskStatus.newBuilder();
		//ExecutorService executorService;
		int p;
		
		while(true){
			heart_beat_req.setTaskTrackerId(TT_id);
			
			try {
				TimeUnit.MILLISECONDS.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if(!reduce_phase){
				p = executorPool.getActiveCount();
				heart_beat_req.setNumMapSlotsFree(nThreads - p);
				System.out.println("req pool free map" + (nThreads - p) );
				System.out.println("get pool size" + executorPool.getPoolSize() + " core "+ executorPool.getCorePoolSize());
				heart_beat_req.setNumReduceSlotsFree(-1);
			}else{
				p = executorPool.getActiveCount();
				heart_beat_req.setNumMapSlotsFree(-1);
				heart_beat_req.setNumReduceSlotsFree(nThreads - p);
				System.out.println("req pool free map" + (nThreads - p));
			}
			
			//if( map_status.size()!=0 ){
			heart_beat_req.clearMapStatus();
			heart_beat_req.clearReduceStatus();
				for( MapReduce.MapTaskStatus mts : map_status  ){
					if(mts!=null){
						heart_beat_req.addMapStatus(mts);
					}
				}
				//heart_beat_req.removeMapStatus(0);
				System.out.println("req map stat cnt: " + heart_beat_req.getMapStatusCount());
			//}
			//if( reduce_status.size()!=0 ){
				for( MapReduce.ReduceTaskStatus rts : reduce_status  ){
					if(rts!=null){
						heart_beat_req.addReduceStatus(rts);
					}
				}
				System.out.println("req red stat cnt: " + heart_beat_req.getReduceStatusCount());
			//}
			
			//send req
			try{
				String s = getLookupName(jobtracker_ip,1099, "JobTrackerImpl");
				IJobTracker stub = (IJobTracker)Naming.lookup(s);
				
				byte[] res =  stub.heartBeat(heart_beat_req.build().toByteArray());
				if(res!=null){
					MapReduce.HeartBeatResponse heart_beat_res = MapReduce.HeartBeatResponse.parseFrom(res);
					System.out.println("res map task cnt" + heart_beat_res.getMapTasksCount());
					if( heart_beat_res.getMapTasksCount()!=0 ){
						for( MapReduce.MapTaskInfo mti: heart_beat_res.getMapTasksList()  ){
							map_queue.add(mti);
						//	System.out.println("testing in mq : "+ mti.getTaskId());
						}
						System.out.println("tt mq size: "+ map_queue.size());
					}
					System.out.println("res red task cnt" + heart_beat_res.getReduceTasksCount());
					System.out.println("MQ SZ: " + map_queue.size());
					if(map_queue == null)
						System.out.println("NULL");
					if( (map_queue!=null) &&  (map_queue.size() == 0)  ){/*&&  heart_beat_res.getReduceTasksCount()!=0*/
						 /* */
						reduce_phase = true;
						for( MapReduce.ReducerTaskInfo rti: heart_beat_res.getReduceTasksList()  ){
							reduce_queue.add(rti);
						}
					}
					String out_file="";
					int task_id,job_id=0;
					String map_name, reduce_name;
					map_futures.clear();
					while((map_queue!=null) &&  (!map_queue.isEmpty() )  ){/*&& (executorPool.getActiveCount()!=0)*/
						map_task_info = map_queue.remove();
						task_id = map_task_info.getTaskId();
						job_id = map_task_info.getJobId();
						map_name = map_task_info.getMapName();
						out_file = getMapOutputFile(task_id,job_id);
						//System.out.println("to worker thread " + task_id);
						WorkerThread mapper = new WorkerThread(true, map_name, TT_id, task_id , map_task_info.getInputBlocks(0), out_file, job_id, null, namenode_ip);
						@SuppressWarnings("unchecked")
						Future<List<Integer> > t_id = (Future<List<Integer> >) executorPool.submit(mapper);
						map_futures.add(t_id);
					}
					
					if(!reduce_phase ){
						java.util.List<Integer>  l = new ArrayList<Integer>();
						System.out.println("map features "+map_futures.size());
						for (Future<List<Integer>> future:map_futures) {
							l = future.get();
							task_id = l.get(0);
							job_id = l.get(1);
							
							map_task_status.setTaskId(task_id);
							map_task_status.setJobId(job_id);
							if( future.isDone() ){
								map_task_status.setTaskCompleted(true);
								//1 job at a time;
								
								out_file = getMapOutputFile(task_id, job_id);
								map_task_status.setMapOutputFile(out_file);
								System.out.println("map job " + job_id + " task "+ task_id + " is done. out_file: " + out_file);
								//ouptut file
							}else{
								map_task_status.setTaskCompleted(false);
							}
							map_status[task_id] = map_task_status.build();
						}
					}else{
						reduce_futures.clear();
						while( !(reduce_queue==null) && !(reduce_queue.isEmpty()) ){
							reduce_task_info = reduce_queue.remove();
							task_id = reduce_task_info.getTaskId();
							job_id = reduce_task_info.getJobId();
							reduce_name = reduce_task_info.getReducerName();
							reduce_files.clear();
							System.out.println(reduce_task_info.getMapOutputFilesCount() + " rf cnt ");
							for( String rf : reduce_task_info.getMapOutputFilesList() ){
								reduce_files.add(rf);
							}
							WorkerThread reducer = new WorkerThread(false, reduce_name, TT_id, task_id ,null, out_file, job_id, reduce_files, namenode_ip);
							@SuppressWarnings("unchecked")
							Future<List<Integer>> t_id = (Future<List<Integer> >) executorPool.submit(reducer);
							reduce_futures.add(t_id);
						}
						
						List<Integer>  l;
						for (Future<List<Integer>> future:reduce_futures) {
							l = future.get();
							task_id = l.get(0);
							job_id = l.get(1);
							
							reduce_task_status.setTaskId(task_id);
							reduce_task_status.setJobId(job_id);
							if( future.isDone() ){
								reduce_task_status.setTaskCompleted(true);
								//1 job at a time;
								
								System.out.println("reduce task done: "+ job_id + " task " + task_id);
								
								//ouptut file
							}else{
								reduce_task_status.setTaskCompleted(false);
							}
							reduce_status[task_id] = reduce_task_status.build();
						}
					}
					
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
	
	}
	
	public static boolean reduce_phase = false;
	public static Integer TT_id;
	public static Integer nThreads = 2;
	public static String jobtracker_ip, namenode_ip;
	public static String MapName, ReduceName;
	public static Collection<Future<List<Integer>>> map_futures = new ArrayList<Future<List<Integer>>>();
	public static Collection<Future<List<Integer>>> reduce_futures = new ArrayList<Future<List<Integer>>>();
	public static Queue<MapReduce.MapTaskInfo> map_queue = new LinkedList<MapReduce.MapTaskInfo>() ;
	public static Queue<MapReduce.ReducerTaskInfo> reduce_queue = new LinkedList<MapReduce.ReducerTaskInfo>() ;
	public static MapReduce.MapTaskStatus[] map_status = new MapReduce.MapTaskStatus[10000];
	public static MapReduce.ReduceTaskStatus[] reduce_status = new MapReduce.ReduceTaskStatus[10000];
	public static List<String> reduce_files = new ArrayList<String>();
}


//executor.shutdown();
/*
    executorPool.shutdown();
    //shut down the monitor thread
    Thread.sleep(5000);
    monitor.shutdown();
*/		

