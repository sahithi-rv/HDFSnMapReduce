
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.protobuf.InvalidProtocolBufferException;


public class NameNodeImpl implements INameNode{

	public static String getLookupName(String ip,int port,  String obj){
		String s="";
		s+="rmi://";
		s+=ip;
		/*s+=":";
		s+=Integer.toString(port);*/
		s+="/";
		s+=obj;
		return s;
	}
	
	public static void addEntryToNNFile(NameNodeStruct nn_struct){

		int i=0;
		String f_name = nn_struct.filename;
		Vector<Integer> chunk_nums ;//= new ArrayList<Integer>();
		chunk_nums = nn_struct.chunk_numbers;
		
		String new_line = "";
		new_line += f_name;
		
		for(int chunk_nm: chunk_nums){
			
			new_line += " ";
			new_line += Integer.toString(chunk_nm);
		}
		
		try(FileWriter fw = new FileWriter(namenode_file, true); 
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw))
		{
			out.println(new_line);
		}catch(Exception e){
			e.printStackTrace();
		}	
	}
	
	// writes data from namenodetsruct to namenode.data file
	public static void updateNamenodeFile(NameNodeStruct nn_struct){
		
		try{
			
			InputStream fd = new FileInputStream(new File(namenode_file));
			BufferedReader reader = new BufferedReader( new InputStreamReader(fd) );
			String last_line = "", current_line;

			while( (current_line = reader.readLine())!= null ){
				last_line = current_line;
			}		
		
			addEntryToNNFile(nn_struct);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings({ "resource", "unchecked" })
	public static void parseFile() throws IOException{
		File f = new File(namenode_file);
		f.createNewFile();
		InputStream fd = new FileInputStream(f);
		BufferedReader reader = new BufferedReader( new InputStreamReader(fd) );
		String current_line;
		try{
			int j = 1;
			NameNodeStruct temp_struct = new NameNodeStruct();
			
			while( (current_line = reader.readLine()) != null ){
				if( current_line.length() > 0){
					
					String parsed_string[] = current_line.split(" ");
					temp_struct.filename =  parsed_string[0];
					for( int i = 1;i<parsed_string.length; i++){
						temp_struct.chunk_numbers.add(Integer.parseInt( Integer.toString(temp_struct.handle) + Integer.toString(i)));				
					}
					chunk_num+=parsed_string.length-2;
					temp_struct.handle = j;
					namenode_struct.put(temp_struct.filename,temp_struct);
					j++;
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}	
	}
	
	public NameNodeImpl(){

	}
	
	@SuppressWarnings("null")
	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
	
		InputStream in = new ByteArrayInputStream(inp);
		try {
			Hdfs.OpenFileRequest file_req = Hdfs.OpenFileRequest.parseFrom(in);
			Hdfs.OpenFileResponse.Builder file_res = Hdfs.OpenFileResponse.newBuilder();
			
			String fname = file_req.getFileName();
						
			System.out.println("open file " + fname);
			if( file_req.getForRead() ){
				// read from block numbers
				file_res.setStatus(1);
			}else{
				file_res.setStatus(0);
				// write to file	
			}
			
			if( namenode_struct.containsKey(fname) ){
				NameNodeStruct temp_struct;
				temp_struct = namenode_struct.get(fname);
				file_res.setHandle(temp_struct.handle);
				
				file_res.addAllBlockNums(temp_struct.chunk_numbers);
			}else{
				NameNodeStruct temp_struct = new NameNodeStruct();
				// create new entry in the name node struct
				temp_struct.filename = fname;
				temp_struct.handle = namenode_struct.size() + 1;
				//temp_struct.chunk_numbers = null;
				// chunk numbers in assign block
				namenode_struct.put(fname, temp_struct);
				handle_to_filename.put(temp_struct.handle, temp_struct.filename);
			//	updateNamenodeFile(temp_struct);
				file_res.setHandle(temp_struct.handle);
				
				//file_res.addAllBlockNums(temp_struct.chunk_numbers);
			}
			
			return file_res.build().toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		
		try{
			Hdfs.CloseFileRequest close_file_req = Hdfs.CloseFileRequest.parseFrom(inp);
			Hdfs.CloseFileResponse.Builder close_file_res = Hdfs.CloseFileResponse.newBuilder();
			
			int file_handle = close_file_req.getHandle();
			String file_name = handle_to_filename.get(file_handle);
			NameNodeStruct temp_struct = new NameNodeStruct();
			temp_struct = namenode_struct.get(file_name);
			updateNamenodeFile(temp_struct);
			
			close_file_res.setStatus(1);
			return (close_file_res.build().toByteArray());
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public static int getfirstDigit(int n) {
		  while (n < -9 || 9 < n)
			  n /= 10;
		  return Math.abs(n);
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) {
		
		InputStream in = new ByteArrayInputStream(inp);
		
		try {
			
			Hdfs.BlockLocationRequest block_loc_req = Hdfs.BlockLocationRequest.parseFrom(in);
			
			List<Integer> block_nums= block_loc_req.getBlockNumsList();
			
			Hdfs.BlockLocationResponse.Builder block_loc_res = Hdfs.BlockLocationResponse.newBuilder() ;
			Hdfs.BlockLocations.Builder block_loc = Hdfs.BlockLocations.newBuilder();
			//assumption: block number is unique over all data nodes
			
			for( int nums : block_nums ){
				
				block_loc.clear();
				block_loc.setBlockNumber(nums);
				
				for(int j = 1;j<=DN;j++){
					if( block_data.containsKey(j) ){
						Hdfs.BlockReportRequest block_report_req = block_data.get(j);
						List<Integer> block_num_list = block_report_req.getBlockNumbersList();
						if( block_num_list.contains(nums) ){
							block_loc.addLocations(block_report_req.getLocation());
						}
					}
					
				}
				block_loc_res.addBlockLocations(block_loc);
			}
			block_loc_res.setStatus(1);
			return block_loc_res.build().toByteArray();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static ArrayList<Integer> getRandomNums() {
	    Integer[] arr = new Integer[DN];
	    
	    ArrayList<Integer> list = new ArrayList<Integer>();
	    for (int i = 1; i <= arr.length; i++) {
	        arr[i-1] = i;
	    }
	    Collections.shuffle(Arrays.asList(arr));
	    
	    for(int i=0; i<REP; i++){
	    	 list.add(arr[i]);
	    }
	    return list;

	}
	
	@SuppressWarnings({ "unused", "null" })
	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		InputStream in = new ByteArrayInputStream(inp);
		try {
			Hdfs.AssignBlockRequest assign_bloc_req = Hdfs.AssignBlockRequest.parseFrom(in);
			int file_handle = assign_bloc_req.getHandle();
			String file_name = "";
			
			Hdfs.AssignBlockResponse.Builder assign_bloc_res = Hdfs.AssignBlockResponse.newBuilder() ;
		
			for( Map.Entry<String, NameNodeStruct> entry: namenode_struct.entrySet() ){
				if(entry.getValue().handle==file_handle){
					file_name = entry.getKey();
					break;
				}
			}
			
			
			chunk_num++ ;
			
			// = namenode_struct.get(file_name).chunk_numbers.size()+1;
			namenode_struct.get(file_name).chunk_numbers.add(chunk_num);
		
			Hdfs.BlockLocations.Builder bloc_loc = Hdfs.BlockLocations.newBuilder();
			bloc_loc.setBlockNumber(chunk_num);
			ArrayList<Integer> list = getRandomNums();
			
			Hdfs.BlockReportRequest bloc_rep_req;
			
			for(int i: list){
				bloc_rep_req = block_data.get(i);
				bloc_loc.addLocations(bloc_rep_req.getLocation());
			}
			
			assign_bloc_res.setStatus(1);
			assign_bloc_res.setNewBlock(bloc_loc);
			return assign_bloc_res.build().toByteArray();
			
		}catch(Exception e){
			
		}
		return null;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		Hdfs.ListFilesResponse.Builder list_file_res = Hdfs.ListFilesResponse.newBuilder();
		list_file_res.setStatus(1);
		for( Map.Entry<String, NameNodeStruct> entry: namenode_struct.entrySet() ){
			list_file_res.addFileNames(entry.getKey());
		}
		return list_file_res.build().toByteArray();
		
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		InputStream in = new ByteArrayInputStream(inp);
		Hdfs.BlockReportResponse.Builder block_report_res = Hdfs.BlockReportResponse.newBuilder();
		
		try {
			Hdfs.BlockReportRequest block_report_req = Hdfs.BlockReportRequest.parseFrom(in);
			block_data.put(block_report_req.getId(), block_report_req);
			for( int num: block_report_req.getBlockNumbersList()){
				block_report_res.addStatus(1);
			}
			return block_report_res.build().toByteArray();
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		
		try {
			Hdfs.HeartBeatRequest heart_beat_req = Hdfs.HeartBeatRequest.parseFrom(inp);
			int dn_id = 0;
			dn_id = heart_beat_req.getId();
			
			//heart beat response
			Hdfs.HeartBeatResponse.Builder heart_beat_res = Hdfs.HeartBeatResponse.newBuilder();
			if( heart_beat_req != null){
				heart_beat_res.setStatus(1);
			}
			return heart_beat_res.build().toByteArray();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	public static void main(String args[]) throws RemoteException{
		namenode_ip = args[0];
		nn_port = Integer.parseInt(args[1]);
		DN = Integer.parseInt(args[2]);
		REP = Integer.parseInt(args[3]);
		try{
			parseFile();
			System.out.println("parsed file: Filename ChunkNumbers Handle");
		}catch(Exception e){
			e.printStackTrace();
		}
		NameNodeImpl obj = new NameNodeImpl();
		INameNode req_skel = (INameNode) UnicastRemoteObject.exportObject(obj, nn_port);
		//Registry registry = LocateRegistry.getRegistry();
		try {
			String s = getLookupName(namenode_ip,1099, "NameNodeImpl");
		//	System.out.println(s);
			Naming.rebind("NameNodeImpl", req_skel);
		} catch (RemoteException e) {
			
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static int chunk_num = 0;
	public static String namenode_file = "namenode.data";
	public static Map<String, NameNodeStruct> namenode_struct = new HashMap<String, NameNodeStruct>();
	public static Map<Integer, String> handle_to_filename = new HashMap<Integer, String>();
	public static int DN , REP;
	public static Map<Integer,Hdfs.BlockReportRequest> block_data = new HashMap<Integer, Hdfs.BlockReportRequest>();
	public static String namenode_ip;
	public static Integer nn_port;
	//public static Vector<NameNodeStruct> namenode_struct = new Vector<NameNodeStruct>();
}
