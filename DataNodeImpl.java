import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

public class DataNodeImpl implements IDataNode {

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
	
	@SuppressWarnings("resource")
	public static void parseFile() throws IOException{
	
		String dn_file = "";
		dn_file+="datanode-";
		dn_file+= DN_id.toString();
		File f = new File(dn_file);
		f.createNewFile();
		InputStream fd = new FileInputStream(f);
		BufferedReader reader = new BufferedReader( new InputStreamReader(fd) );
		String current_line;
		try{
			while( (current_line = reader.readLine()) != null ){
				
				if( current_line.length() > 0){
					String parsed_string[] = current_line.split(" ");
					datanode_struct.put(Integer.parseInt(parsed_string[0]), parsed_string[1]);
				}
			}
		}catch(Exception e){
			
		}
	}
	
	public DataNodeImpl(){
		
	}
	
	public static String readFile(String path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
			}
	
	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException {
		InputStream in = new ByteArrayInputStream(inp);
		Hdfs.ReadBlockResponse.Builder read_block_res = Hdfs.ReadBlockResponse.newBuilder();
		
		try {
			//get block number from req message
			Hdfs.ReadBlockRequest read_block_req = Hdfs.ReadBlockRequest.parseFrom(in);
			int block_num = read_block_req.getBlockNumber();
			
			String block_name = datanode_struct.get(block_num);
			read_block_res.setStatus(1);
			
			InputStream fd = new FileInputStream(new File(block_name));
			ByteString data = ByteString.readFrom(fd);
			read_block_res.addData(data);
			return read_block_res.build().toByteArray();
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public void update_file(Integer block_num){
		String file_name = "datanode-";
		file_name+= DN_id.toString();
		String line = "";
		line += block_num.toString();
		line += " ";
		line += "file-";
		line += block_num.toString();
		
		try(FileWriter fw = new FileWriter(file_name, true); 
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw))
		{
			
			out.println(line);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		datanode_struct.put(block_num, "file-"+block_num.toString());
		
	}

	public byte[] writeBlockToSelf(byte[] inp) throws RemoteException{

		InputStream in = new ByteArrayInputStream(inp);
		Hdfs.WriteBlockRequest write_block_req;
		try {
			write_block_req = Hdfs.WriteBlockRequest.parseFrom(in);
			
			Hdfs.BlockLocations block_loc =  write_block_req.getBlockInfo();
			Hdfs.WriteBlockResponse.Builder write_block_res = Hdfs.WriteBlockResponse.newBuilder();
			
			List<ByteString> data = write_block_req.getDataList();
		
			int block_num = block_loc.getBlockNumber();
			
			// update datanode file and datanode_struct
			update_file(block_num);
			
			//open file and write to it
			String file_name = datanode_struct.get(block_num);
			try(FileWriter fw = new FileWriter(file_name, true); 
					BufferedWriter bw = new BufferedWriter(fw);
					PrintWriter out = new PrintWriter(bw))
			{
				String final_data = "";
				for( ByteString chunk : data ){
					final_data += chunk.toStringUtf8();
					
				}
				out.println(final_data);
				write_block_res.setStatus(1);
				return write_block_res.build().toByteArray();
			}catch(Exception e){
				e.printStackTrace();
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	
		return null;
		
	}
	

	
	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		
		InputStream in = new ByteArrayInputStream(inp);
		
		
		try {
			//get block number from req message
			Hdfs.WriteBlockRequest write_block_req = Hdfs.WriteBlockRequest.parseFrom(in);
			Hdfs.BlockLocations block_loc =  write_block_req.getBlockInfo();
			//List<ByteString> data = write_block_req.getDataList();
			
			List<Hdfs.DataNodeLocation> data_node_loc = block_loc.getLocationsList();
			Hdfs.WriteBlockResponse.Builder wbr = Hdfs.WriteBlockResponse.newBuilder();
			String data_node_ip;
			int data_node_port, data_node_id;
			for( Hdfs.DataNodeLocation dnlocs : data_node_loc ){
				data_node_ip=dnlocs.getIp();
				data_node_port = dnlocs.getPort();
				data_node_id = data_node_port%10;
				
				System.out.println("Writing block " + block_loc.getBlockNumber() + " to datanode "+ data_node_id);
				
				String s = getLookupName(data_node_ip, 1099, "DataNodeImpl-" + Integer.toString(data_node_id));
				IDataNode stub = (IDataNode)Naming.lookup(s);
				
				byte[] res =  stub.writeBlockToSelf(inp);
				Hdfs.WriteBlockResponse write_block_res = Hdfs.WriteBlockResponse.parseFrom(res) ;
				if( write_block_res.getStatus()!=1 ){
					wbr.setStatus(0);
					return wbr.build().toByteArray();
				}
			}
			wbr.setStatus(1);
			return wbr.build().toByteArray();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return null;
	}
	
	@SuppressWarnings("null")
	public static void main(String args[]) throws RemoteException{
		DN_ip = args[0];
		DN_port = Integer.parseInt(args[1]);
		
		namenode_ip = args[2];
		NN_port = Integer.parseInt(args[3]);
		DNs = Integer.parseInt(args[4]);
		DN_id = Integer.parseInt(args[5]);
		
		DataNodeImpl obj = new DataNodeImpl();
		IDataNode req_skel = (IDataNode) UnicastRemoteObject.exportObject(obj, DN_port);
		//Registry registry = LocateRegistry.getRegistry();
		try {
			Naming.rebind("DataNodeImpl-" + Integer.toString(DN_id), req_skel);
			System.out.println("datanode name: " + "DataNodeImpl-" + Integer.toString(DN_id) );
			System.out.println("Bound");
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		
		try {
			parseFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Hdfs.HeartBeatRequest.Builder heart_beat_req = Hdfs.HeartBeatRequest.newBuilder();
		heart_beat_req.setId(DN_id);
	
		Hdfs.BlockReportRequest.Builder block_report_req = Hdfs.BlockReportRequest.newBuilder();
		Hdfs.DataNodeLocation.Builder dn_loc = Hdfs.DataNodeLocation.newBuilder();
		Hdfs.BlockReportResponse block_rep_res ;
		while(true){
			
			
			
			block_report_req.setId(DN_id);
			
			dn_loc.setIp(DN_ip);
			dn_loc.setPort(DN_port);
			block_report_req.setLocation(dn_loc);
			block_report_req.clearBlockNumbers();
			if(! (datanode_struct.isEmpty()) ){
				for( Map.Entry<Integer, String> entry: datanode_struct.entrySet() ){
					block_report_req.addBlockNumbers(entry.getKey());
				}
			}
			
			
			String s = getLookupName(namenode_ip, 1099,"NameNodeImpl");
			
			try {
				Registry registry = LocateRegistry.getRegistry(namenode_ip,1099);
			
				INameNode stub = (INameNode)Naming.lookup(s);
				//INameNode stub = (INameNode)registry.lookup("NameNodeImpl");
				
				byte[] b = stub.blockReport(block_report_req.build().toByteArray());
				block_rep_res = Hdfs.BlockReportResponse.parseFrom(b);
				
				stub.heartBeat(heart_beat_req.build().toByteArray() );
				TimeUnit.SECONDS.sleep(2);
			}catch(Exception e){
				e.printStackTrace();
				//break;	
			}
			
			
		}
		
	}
	
	public static Map<Integer, String> datanode_struct = new HashMap<Integer, String>();
	public static Integer DN_id;
	public static String DN_ip;
	public static Integer NN_port;
	public static Integer DN_port;
	public static Integer DNs;
	public static String namenode_ip;

}

/*
 * //connect to namenode and get block locations
			INameNode stub = (INameNode)Naming.lookup(name_node);
			Hdfs.BlockLocationRequest.Builder block_loc_req = null;
			block_loc_req.addBlockNums(block_num);
			inp = stub.getBlockLocations(block_loc_req.build().toByteArray());
			InputStream block_loc_in = new ByteArrayInputStream(inp);
			Hdfs.BlockLocationResponse block_loc_res = Hdfs.BlockLocationResponse.parseFrom(block_loc_in);
 * */



