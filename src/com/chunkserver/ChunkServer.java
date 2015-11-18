package com.chunkserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
//import java.util.Arrays;

import com.client.Client;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ClientChunkServerConfigFile = "ClientChunkServerConfig.txt";
	public final static String MasterChunkServerConfigFile = "MasterChunkServerConfig.txt";
	public final static String [] MasterCSconfigFiles = {
			"MasterCS1Config.txt",
			"MasterCS2Config.txt",
			"MasterCS3Config.txt"
	};
	public final static String [] ClientCSconfigFiles = {
			"ClientCS1Config.txt",
			"ClientCS2Config.txt",
			"ClientCS3Config.txt"
	};
	public final static int ChunkSize = 1024 * 1024; //1024 KB chunk sizes
	
	//Used for the file system
	public static long counter;
	
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	// Master Connection
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File dir = new File(filePath);
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	public void ReadAndProcessRequests()
	{	
		//Used for communication with the Client via the network
		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
		ServerSocket commChanel = null;
		ObjectOutputStream WriteOutput = null;
		ObjectInputStream ReadInput = null;
		
		try {
			//Allocate a port and write it to the config file for the Client to consume
			commChanel = new ServerSocket(ServerPort);
			ServerPort=commChanel.getLocalPort();
			PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientChunkServerConfigFile));
			outWrite.println("localhost:"+ServerPort);
			outWrite.close();
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}
		
		boolean done = false;
		Socket ClientConnection = null;  //A client's connection to the server

		while (!done){
			try {
				ClientConnection = commChanel.accept();
				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				
				//Use the existing input and output stream as long as the client is connected
				while (!ClientConnection.isClosed()) {
					int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					if (payloadsize == -1) 
						break;
					int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					switch (CMD){
					case CreateChunkCMD:
						String chunkhandle = createChunk();
						byte[] CHinbytes = chunkhandle.getBytes();
						WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
						WriteOutput.write(CHinbytes);
						WriteOutput.flush();
						break;

					case ReadChunkCMD:
						int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
						byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						String ChunkHandle = (new String(CHinBytes)).toString();
						
						byte[] res = readChunk(ChunkHandle, offset, payloadlength);
						
						if (res == null)
							WriteOutput.writeInt(ChunkServer.PayloadSZ);
						else {
							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
							WriteOutput.write(res);
						}
						WriteOutput.flush();
						break;

					case WriteChunkCMD:
						offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
						chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
						CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();

						//Call the writeChunk command
						if (writeChunk(ChunkHandle, payload, offset))
							WriteOutput.writeInt(ChunkServer.TRUE);
						else WriteOutput.writeInt(ChunkServer.FALSE);
						
						WriteOutput.flush();
						break;

					default:
						System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
						break;
					}
				}
			} catch (IOException ex){
				System.out.println("Client Disconnected");
			} finally {
				try {
					if (ClientConnection != null)
						ClientConnection.close();
					if (ReadInput != null)
						ReadInput.close();
					if (WriteOutput != null) WriteOutput.close();
				} catch (IOException fex){
					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
					fex.printStackTrace();
				}
			}
		}
	}
	
	public void ClientConnection() {
		Runnable clientTask = new Runnable() {
			public void run() {
				ReadAndProcessClientRequests();
			}
			public void ReadAndProcessClientRequests()
			{	
				//Used for communication with the Client via the network
				int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
				ServerSocket commChanel = null;
				ObjectOutputStream WriteOutput = null;
				ObjectInputStream ReadInput = null;
				
				try {
					//Allocate a port and write it to the config file for the Client to consume
					commChanel = new ServerSocket(ServerPort);
					ServerPort=commChanel.getLocalPort();
					PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientChunkServerConfigFile));
					outWrite.println("localhost:"+ServerPort);
					outWrite.close();
				} catch (IOException ex) {
					System.out.println("Error, failed to open a new socket to listen on.");
					ex.printStackTrace();
				}
				
				boolean done = false;
				Socket ClientConnection = null;  //A client's connection to the server

				while (!done){
					try {
						ClientConnection = commChanel.accept();
						ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
						WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
						
						//Use the existing input and output stream as long as the client is connected
						while (!ClientConnection.isClosed()) {
							int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							if (payloadsize == -1) 
								break;
							int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							switch (CMD){
							case CreateChunkCMD:
								String chunkhandle = createChunk();
								byte[] CHinbytes = chunkhandle.getBytes();
								WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
								WriteOutput.write(CHinbytes);
								WriteOutput.flush();
								break;

							case ReadChunkCMD:
								int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
								if (chunkhandlesize < 0)
									System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
								byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
								String ChunkHandle = (new String(CHinBytes)).toString();
								
								byte[] res = readChunk(ChunkHandle, offset, payloadlength);
								
								if (res == null)
									WriteOutput.writeInt(ChunkServer.PayloadSZ);
								else {
									WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
									WriteOutput.write(res);
								}
								WriteOutput.flush();
								break;

							case WriteChunkCMD:
								offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
								chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
								if (chunkhandlesize < 0)
									System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
								CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
								ChunkHandle = (new String(CHinBytes)).toString();

								//Call the writeChunk command
								if (writeChunk(ChunkHandle, payload, offset))
									WriteOutput.writeInt(ChunkServer.TRUE);
								else WriteOutput.writeInt(ChunkServer.FALSE);
								
								WriteOutput.flush();
								break;

							default:
								System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
								break;
							}
						}
					} catch (IOException ex){
						System.out.println("Client Disconnected");
					} finally {
						try {
							if (ClientConnection != null)
								ClientConnection.close();
							if (ReadInput != null)
								ReadInput.close();
							if (WriteOutput != null) WriteOutput.close();
						} catch (IOException fex){
							System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
							fex.printStackTrace();
						}
					}
				}
			}
		};
		Thread clientThread = new Thread(clientTask);
		clientThread.start();
	}
	
	public void MasterConnection() {
		Runnable masterTask = new Runnable() {
			public void run() {
				ReadAndProcessMasterRequests();
			}
			public void ReadAndProcessMasterRequests()
			{	
				// Connect to master
				int ServerPort = 0;
				Socket MasterSocket = null;
				ObjectOutputStream WriteOutput = null;
				ObjectInputStream ReadInput = null;
				
				try {
					BufferedReader binput = new BufferedReader(new FileReader(MasterChunkServerConfigFile));
					String port = binput.readLine();
					port = port.substring( port.indexOf(':')+1 );
					ServerPort = Integer.parseInt(port);
					
					MasterSocket = new Socket("127.0.0.1", ServerPort);
					WriteOutput = new ObjectOutputStream(MasterSocket.getOutputStream());
					ReadInput = new ObjectInputStream(MasterSocket.getInputStream());
				} catch (FileNotFoundException e) {
					System.out.println("Error (ChunkServer), the config file "+ MasterChunkServerConfigFile +" containing the port of the ChunkServer is missing.");
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				boolean done = false;

				while (!done){
					try {//Use the existing input and output stream as long as the client is connected
						while (!MasterSocket.isClosed()) {
							int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							if (payloadsize == -1) 
								break;
							int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							switch (CMD){
							case CreateChunkCMD:
								String chunkhandle = createChunk();
								byte[] CHinbytes = chunkhandle.getBytes();
								WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
								WriteOutput.write(CHinbytes);
								WriteOutput.flush();
								break;

							case ReadChunkCMD:
								int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
								if (chunkhandlesize < 0)
									System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
								byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
								String ChunkHandle = (new String(CHinBytes)).toString();
								
								byte[] res = readChunk(ChunkHandle, offset, payloadlength);
								
								if (res == null)
									WriteOutput.writeInt(ChunkServer.PayloadSZ);
								else {
									WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
									WriteOutput.write(res);
								}
								WriteOutput.flush();
								break;

							case WriteChunkCMD:
								offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
								byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
								chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
								if (chunkhandlesize < 0)
									System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
								CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
								ChunkHandle = (new String(CHinBytes)).toString();

								//Call the writeChunk command
								if (writeChunk(ChunkHandle, payload, offset))
									WriteOutput.writeInt(ChunkServer.TRUE);
								else WriteOutput.writeInt(ChunkServer.FALSE);
								
								WriteOutput.flush();
								break;

							default:
								System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
								break;
							}
						}
					} catch (IOException ex){
						System.out.println("Client Disconnected");
					} finally {
						try {
							if (MasterSocket != null)
								MasterSocket.close();
							if (ReadInput != null)
								ReadInput.close();
							if (WriteOutput != null) WriteOutput.close();
						} catch (IOException fex){
							System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
							fex.printStackTrace();
						}
					}
				}
			}
		};
		Thread masterThread = new Thread(masterTask);
		masterThread.start();
	}

	public static void main(String args[])
	{
		ChunkServer cs = new ChunkServer();
//		cs.ReadAndProcessRequests();
		cs.MasterConnection();
		cs.ClientConnection();
	}
}
