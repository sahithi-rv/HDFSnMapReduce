import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

class WorkerThread implements  Callable<List<Integer>> { /*Runnable,*/ 
      
     public WorkerThread(boolean mapr, String jar, int tt, int t, MapReduce.BlockLocations bl, String file, int j, List<String> rf, String nn_ip ){  
    	 this.mr = mapr;
    	 this.JarName = jar;
    	 this.tt_id = tt;
    	 this.task_id = t;
    	 this.bloc_loc = bl;
    	 this.map_out_file = file;
    	 this.job_id = j;
    	 this.reduce_files = rf;
    	 this.namenode_ip = nn_ip;
     } 
     
     
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
     
    @Override
 	public List<Integer> call() throws Exception {
    	
    	 System.out.println(Thread.currentThread().getName()+" (Start) tt = "+ Integer.toString(tt_id));  
         // processmessage();//call processmessage method that sleeps the thread for 2 seconds  
          
          try {
         	String data = "";
         
         	// invoking mapper method
         	Class<?> cls = Class.forName(this.JarName);
 			/*@SuppressWarnings("rawtypes")
 			Class[] = new Class[] { String[].class };*/
         	if( mr ){
         		data = readBlock();
 				Method map = cls.getDeclaredMethod("map", String.class);
 				MapImpl mi = new MapImpl();
 				String s =(String) map.invoke(mi,data );
 				try(FileWriter fw = new FileWriter(map_out_file, true); 
 						BufferedWriter bw = new BufferedWriter(fw);
 						PrintWriter out = new PrintWriter(bw))
 				{	
 					
 					out.print(s);
 				}catch(Exception e){
 					e.printStackTrace();
 				}
 				
 				Helper helper = new Helper(namenode_ip);
 				try {
 					helper.put_file(map_out_file);
 				} catch (IOException e) {
 					// TODO Auto-generated catch block
 					e.printStackTrace();
 				}
 				
         	}else{
         		Helper helper = new Helper(namenode_ip);
         		System.out.println("reduce files sz: " + reduce_files.size());
         		for( String rf : this.reduce_files ){
         			data+= helper.get_file(rf);
         			System.out.println("data : " + data);
         		}
 				Method reduce= cls.getDeclaredMethod("reduce", String.class);
 				ReduceImpl mi = new ReduceImpl();
 				String s = (String)reduce.invoke(mi,data );
 				System.out.println(s);
         	}
 		} catch (ClassNotFoundException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} catch (NoSuchMethodException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} catch (SecurityException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} catch (IllegalAccessException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} catch (IllegalArgumentException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} catch (InvocationTargetException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
          
        System.out.println(Thread.currentThread().getName()+" (End) tt = "+ Integer.toString(tt_id));//prints thread name  
        List<Integer> l = new ArrayList<Integer>();
        
    //	System.out.println(task_id + " , " + job_id);
    	l.add(task_id);
    	l.add(job_id);
 		return l;
 	}
    
    public String readBlock(){
    	int block_num = this.bloc_loc.getBlockNumber();
		List<MapReduce.DataNodeLocation> dn_locs = this.bloc_loc.getLocationsList();
		String final_data = "";
		//randomly choose a data node
		//Collections.shuffle(dn_locs);
		MapReduce.DataNodeLocation dn = dn_locs.get(0);
		
		//lookup datanode
		int dn_id = dn.getPort()%10;
		String a = getLookupName(dn.getIp(),1099, "DataNodeImpl-"+ dn_id);
		IDataNode dn_stub;
	
		
		// read content of block
		
		//read block request 
		Hdfs.ReadBlockRequest.Builder read_block_req = Hdfs.ReadBlockRequest.newBuilder();
		read_block_req.setBlockNumber(block_num);
		byte[] b;
		try {
			dn_stub = (IDataNode)Naming.lookup(a);
			b = dn_stub.readBlock(read_block_req.build().toByteArray());
			Hdfs.ReadBlockResponse read_block_res = Hdfs.ReadBlockResponse.parseFrom(b);
			List<ByteString> data = read_block_res.getDataList();
			
			
			for( ByteString chunk : data ){
				final_data += chunk.toStringUtf8();
			}
			
			
						
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//read block response
		return final_data;
	
    }
    
    
    
   /*  @Override
     public void run() {  
         System.out.println(Thread.currentThread().getName()+" (Start) tt = "+ Integer.toString(tt_id));  
        // processmessage();//call processmessage method that sleeps the thread for 2 seconds  
         
         try {
        	String data = "";
        	data = readBlock();
        	// invoking mapper method
        	Class<?> cls = Class.forName(this.JarName);
			@SuppressWarnings("rawtypes")
			Class[] argTypes = new Class[] { String[].class };
        	if( mr ){
	        	
				Method map = cls.getDeclaredMethod("map", argTypes);
				MapImpl mi = new MapImpl();
				String s =(String) map.invoke(mi,data );
				try(FileWriter fw = new FileWriter(map_out_file, true); 
						BufferedWriter bw = new BufferedWriter(fw);
						PrintWriter out = new PrintWriter(bw))
				{	
					
					out.print(s);
				}catch(Exception e){
					e.printStackTrace();
				}
				
				Helper helper = new Helper(namenode_ip);
				try {
					helper.put_file(map_out_file);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
        	}else{
        		Helper helper = new Helper(namenode_ip);
        		
        		for( String rf : this.reduce_files ){
        			data+= helper.get_file(rf);
        		}
				Method reduce= cls.getDeclaredMethod("reduce", argTypes);
				ReduceImpl mi = new ReduceImpl();
				String s = (String)reduce.invoke(mi,data );
				System.out.println(s);
        	}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
         System.out.println(Thread.currentThread().getName()+" (End) tt = "+ Integer.toString(tt_id));//prints thread name  
         
     }  */
     
     private boolean mr ;
     private int tt_id;
     private String JarName, namenode_ip;
     private int task_id;
     private MapReduce.BlockLocations bloc_loc;
     private String map_out_file;
     private int job_id;
     private List<String> reduce_files;
     
	
     
     
 }  