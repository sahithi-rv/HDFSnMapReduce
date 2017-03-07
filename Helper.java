import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class Helper {
	
	public Helper(String nn_ip){
		namenode_ip = nn_ip;
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
	
	public static ByteString readChunk(InputStream in, final int chunkSize)
			  throws IOException {
			  final byte[] buf = new byte[chunkSize];
			  int bytesRead = 0;
			  while (bytesRead < chunkSize) {
			    final int count = in.read(buf, bytesRead, chunkSize - bytesRead);
			    if (count == -1) {
			      break;
			    }
			    bytesRead += count;
			  }
			
			  if (bytesRead == 0) {
			    return null;
			  }
			
			  // Always make a copy since InputStream could steal a reference to buf.
			  return ByteString.copyFrom(buf, 0, bytesRead);
	   }
	
	@SuppressWarnings("null")
	public String get_file(String file_name) {
		//lookup namenode
		
		String s = getLookupName(namenode_ip,1099, "NameNodeImpl");
		try {
			INameNode stub = (INameNode)Naming.lookup(s);
			
			//open file 
			Hdfs.OpenFileRequest.Builder open_file_req = Hdfs.OpenFileRequest.newBuilder();
			open_file_req.setFileName(file_name);
			open_file_req.setForRead(true);
			byte[] b = stub.openFile(open_file_req.build().toByteArray());
			try {
				Hdfs.OpenFileResponse open_file_res = Hdfs.OpenFileResponse.parseFrom(b);
				
				//blocnums
				List<Integer> block_nums = open_file_res.getBlockNumsList();
				
				// create block location request
				Hdfs.BlockLocationRequest.Builder block_loc_req = Hdfs.BlockLocationRequest.newBuilder();
				block_loc_req.addAllBlockNums(block_nums);
				System.out.println(block_nums.size());
				//obtain block locations
				b = stub.getBlockLocations(block_loc_req.build().toByteArray());
				
				Hdfs.BlockLocationResponse block_loc_res = Hdfs.BlockLocationResponse.parseFrom(b);
				List<Hdfs.BlockLocations> block_locs = block_loc_res.getBlockLocationsList();
				
				// for every block 
				for( Hdfs.BlockLocations block : block_locs){
					
					int block_num = block.getBlockNumber();
					List<Hdfs.DataNodeLocation> dn_locs = block.getLocationsList();
					
					//randomly choose a data node
					Collections.shuffle(dn_locs);
					Hdfs.DataNodeLocation dn = dn_locs.get(0);
					
					//lookup datanode
					int dn_id = dn.getPort()%10;
					String a = getLookupName(dn.getIp(),1099, "DataNodeImpl-"+ dn_id);
					IDataNode dn_stub = (IDataNode)Naming.lookup(a);
					
					// read content of block
					
					//read block request 
					Hdfs.ReadBlockRequest.Builder read_block_req = Hdfs.ReadBlockRequest.newBuilder();
					read_block_req.setBlockNumber(block_num);
					b = dn_stub.readBlock(read_block_req.build().toByteArray());
					
					//read block response
					Hdfs.ReadBlockResponse read_block_res = Hdfs.ReadBlockResponse.parseFrom(b);
					List<ByteString> data = read_block_res.getDataList();
				
					// write to file
				
					
						String final_data = "";
						for( ByteString chunk : data ){
							final_data += chunk.toStringUtf8();
						}
						return final_data;
					
					
				}
				
				
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	
	@SuppressWarnings("null")
	public void put_file(String file_name) throws IOException {
		
		String s = getLookupName(namenode_ip,1099, "NameNodeImpl");
		try {
			INameNode stub = (INameNode)Naming.lookup(s);
			
			//open file 
			Hdfs.OpenFileRequest.Builder open_file_req = Hdfs.OpenFileRequest.newBuilder();
		
			open_file_req.setFileName(file_name);
			open_file_req.setForRead(false);
			byte[] b = stub.openFile(open_file_req.build().toByteArray());
	
			try{
				//get file handle
				Hdfs.OpenFileResponse open_file_res = Hdfs.OpenFileResponse.parseFrom(b);
				int file_handle = open_file_res.getHandle();
				
				InputStream fd = new FileInputStream(new File(file_name));
				ByteString data;
				
				while( true ){
					data = readChunk(fd, CHUNK_SIZE);
					// assign block corresponding to current chunk of data
					if( data==null )
						break;
					
					Hdfs.AssignBlockRequest.Builder assign_bloc_req = Hdfs.AssignBlockRequest.newBuilder();
					assign_bloc_req.setHandle(file_handle);
					System.out.println( " Assign block for handle" + file_handle);
					b = stub.assignBlock(assign_bloc_req.build().toByteArray());
					
					Hdfs.AssignBlockResponse assign_bloc_res = Hdfs.AssignBlockResponse.parseFrom(b);
					int block_status = assign_bloc_res.getStatus();
					
					if(block_status==1){
						Hdfs.BlockLocations bloc_loc = assign_bloc_res.getNewBlock();
						
						// write block
						Hdfs.WriteBlockRequest.Builder write_block_req = Hdfs.WriteBlockRequest.newBuilder() ;
						write_block_req.addData(data);
						write_block_req.setBlockInfo(bloc_loc);
						
						Hdfs.DataNodeLocation dn_loc = bloc_loc.getLocations(0);
						int dn_id = dn_loc.getPort()%10;
						System.out.println( "New block" + bloc_loc.getBlockNumber()+ " assigned, writing block to datanode "+dn_id );
						String a = getLookupName(dn_loc.getIp(), 1099, "DataNodeImpl-" + dn_id);
						
						IDataNode dn_stub = (IDataNode)Naming.lookup(a);
						b = dn_stub.writeBlock(write_block_req.build().toByteArray());
						
						Hdfs.WriteBlockResponse write_block_res = Hdfs.WriteBlockResponse.parseFrom(b);
						
						if( write_block_res.getStatus() == 0 ){
							System.out.println( "something wrong while writing to block ");
						}
					}	
				}
				
				
			}catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch(FileNotFoundException e){
			e.printStackTrace();
		}
	}
	
	public static String namenode_ip;
	public static int CHUNK_SIZE = 4;
}
