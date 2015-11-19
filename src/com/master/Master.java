package com.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;

import com.chunkserver.ChunkServer;
import com.client.Client;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.client.FileHandle;
import com.client.RID;
import com.master.Transaction.Command;

public class Master {

	public final static String MasterClientConfigFile = "MasterClientConfig.txt";
	
	//Commands recognized by the Master from Client
	public static final int CreateDirCMD = 201;
	public static final int DeleteDirCMD = 202;
	public static final int RenameDirCMD = 203;
	public static final int ListDirCMD = 204;
	public static final int CreateFileCMD = 205;
	public static final int DeleteFileCMD = 206;
	public static final int OpenFileCMD = 207;
	public static final int CloseFileCMD = 208;
	public static final int AppendRecordCMD = 209;
	public static final int DeleteRecordCMD = 210;
	public static final int ReadFirstRecordCMD = 211;
	public static final int ReadLastRecordCMD = 212;
	public static final int ReadNextRecordCMD = 213;
	public static final int ReadPrevRecordCMD = 214;
	
	//TODO: Maintain 3 file names
	public static final String MasterLogFile0 = "MasterLog0.txt";
	public static final String MasterLogFile1 = "MasterLog1.txt";
	public static final String MasterLogFile2 = "MasterLog2.txt";
	protected static final long LogFileSize = 512000;
	protected static final long TransactionMsgSize = 40;
	protected static final int CHECKPOINTS = 3;
	private static int TransactionNum;
	
	public static final int UP = 301;
	public static final int DOWN = 302;
	public static final int WRITTEN = 303;
	public static final int UNWRITTEN = 304;
	private boolean [] ChunkServerAvailability = {true};
	private static final int ChunkServerExpected = 1; // TO BE CHANGED LATER
	private static int ChunkServerCount = 0; // TO BE CHANGED LATER
	
	
	private ServerSocket csCommChannel;
	private Socket csConnection;
	private ObjectOutputStream WriteOutputToCS;
	private ObjectInputStream ReadInputFromCS;
	
	protected static Tree namespace;
	private ArrayList<Log> logs;
	private Log log; 

	//TODO: Update log information when the current log file meets checkpoint(too large)
	
	public Master() {
		// initialize all data structure
		
		DirectoryMD rootdir = new DirectoryMD();
		rootdir.name = "";
		namespace = new Tree(rootdir);
		//TODO: Choose a filename to pass in
//		logs = new Log[3];
//		this.log = new Log(MasterLogFile);
		TransactionNum = 0;
		this.logs = new ArrayList<Log>();
		logs.add(new Log(MasterLogFile0));
		logs.add(new Log(MasterLogFile1));
		logs.add(new Log(MasterLogFile2));
		
		log= logs.get(0);
		
		if(log.logFile.length() > 0){
			//load metadata from log file
			log.Load();
			for(Transaction t: log.transactions.values()){
				t.Redo();
				TransactionNum = Integer.parseInt(t.ID);
			}
		}
		System.out.println("here");
		
	}
	
	public void makeCheckpoint(){
		
		logs.get(1).rename(MasterLogFile2);
		logs.get(0).rename(MasterLogFile1);
//		logs.get(0).rename(MasterLogFile0);
		logs.add(0, new Log(MasterLogFile0));
		logs.remove(logs.size()-1);
		log = logs.get(0);
	}
	
	public FSReturnVals CreateDir(String src, String dirname) {
		//LOGGING: Start transaction
		Transaction T = new Transaction(Command.CreateDir, src, dirname, TransactionNum++);
		log.Start(T);
		System.out.println ( "T(" + T.ID + "):   CreateDir");
		log.AddMessage(T.toString());
		if(!log.Commit(T)){
			makeCheckpoint();
		}
		return T.Redo();
//		
//		// get node
//		Node newNode = GetNode(src);
//
//		if (newNode == null) {
//			return FSReturnVals.SrcDirNotExistent;
//		}
//		
//		// check if directory already exists
//		if (newNode.GetChild(dirname) != null) {
//			return FSReturnVals.DirExists;
//		}
//		
//		
//		// add directory
//		DirectoryMD newDirectory = new DirectoryMD();
//		newDirectory.name = dirname;
//		newNode.AddChild(newDirectory);
//		
//		return FSReturnVals.Success;
	}
	
	// src must have '/' at the end of each directory
	public FSReturnVals DeleteDir(String src, String dirname) {
		
		
		Transaction T = new Transaction(Command.DeleteDir, src, dirname, log.transactions.size()); 
		log.Start(T);
		System.out.println ( "T(" + T.ID + "):   CreateDir");
		log.AddMessage(T.toString());
		if(!log.Commit(T)){
			makeCheckpoint();
		}
		return T.Redo();
		
		// retrieve directory
//		Node byeNode = GetNode(src);
//		if (byeNode == null) {
//			return FSReturnVals.SrcDirNotExistent;
//		}
//		
//		// check if directory exists
//		if (byeNode.GetChild(dirname) == null) {
//			return FSReturnVals.DirDoesNotExist;
//		}
//		
//		// if directory is not empty, CANNOT DELETE!
//		if (!byeNode.GetChild(dirname).GetChildren().isEmpty()) {
//			return FSReturnVals.DirNotEmpty;
//		}
//		
//		// delete current directory
//		byeNode.RemoveChild(dirname);
//		
//		return FSReturnVals.Success;
	}
	
	// src is FULL PATH of directory
	public FSReturnVals RenameDir(String src, String NewName) {
		Transaction T = new Transaction(Command.RenameDir, src, NewName, log.transactions.size()); 
		log.Start(T);
		System.out.println ( "T(" + T.ID + "):   CreateDir");
		log.AddMessage(T.toString());
		if(!log.Commit(T)){
			makeCheckpoint();
		}
		return T.Redo();
		
		
		// check if both give the same number of levels and are all same up to the last one
//		ArrayList<String> path = ParsePath(src);
//		ArrayList<String> newpath = ParsePath(NewName);
//		if (path.size() != newpath.size()) {
//			return FSReturnVals.Fail;
//		}
//		for (int i=0; i < path.size()-1; i++) {
//			if (!path.get(i).equals(newpath.get(i))) {
//				return FSReturnVals.Fail;
//			}
//		}
//		
//		// get node
//		Node reNode = GetNode(src);
//		if (reNode == null) {
//			return FSReturnVals.SrcDirNotExistent;
//		}
//		
//		// rename
//		reNode.SetName(newpath.get(newpath.size()-1));
//		
//		return FSReturnVals.Success;
	}
	
	public String[] ListDir(String tgt) {
		
		Node dirNode = GetNode(tgt);
		if (dirNode == null) {
			return null;
		}
		
		ArrayList<Node> allDescendants = dirNode.GetAllDescendants();
		
		String[] ls = new String[allDescendants.size()];
		for (int i=0; i < ls.length; i++) {
			ls[i] = allDescendants.get(i).GetFullPath();
		}
		
		return ls;
	}
	
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		Transaction T = new Transaction(Command.CreateFile, tgtdir, filename, log.transactions.size()); 
		log.Start(T);
		System.out.println ( "T(" + T.ID + "):   CreateDir");
		log.AddMessage(T.toString());
		if(!log.Commit(T)){
			makeCheckpoint();
		}
		return T.Redo();
		
//		// get node
//		Node newNode = GetNode(tgtdir);
//		if (newNode == null) {
//			return FSReturnVals.SrcDirNotExistent;
//		}
//		
//		// check if directory already exists
//		if (newNode.GetChild(filename) != null) {
//			return FSReturnVals.FileExists;
//		}
//		
//		// add directory
//		FileMD newFile = new FileMD();
//		newFile.name = filename;
//		newNode.AddChild(newFile);
//		
//		return FSReturnVals.Success;
	}
	
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		Transaction T = new Transaction(Command.DeleteFile, tgtdir, filename, log.transactions.size()); 
		log.Start(T);
		System.out.println ( "T(" + T.ID + "):   CreateDir");
		log.AddMessage(T.toString());
		if(!log.Commit(T)){
			makeCheckpoint();
		}
		return T.Redo();
		
//		// retrieve directory
//		Node byeNode = GetNode(tgtdir);
//		
//		// if directory does not exist, return error
//		if (byeNode == null) {
//			return FSReturnVals.SrcDirNotExistent;
//		}
//		
//		// if file does not exist, return error
//		if (byeNode.GetChild(filename) == null) {
//			return FSReturnVals.FileDoesNotExist;
//		}
//		
//		// delete the file
//		byeNode.RemoveChild(filename);
//		
//		return FSReturnVals.Success;
	}
	
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		// if invalid filepath, return error
		Node tempNode = GetNode(FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		ofh.FilePath = FilePath;
		ofh.ChunkServerStatus = null;
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals CloseFile(FileHandle ofh) {
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.BadHandle;
		}
		
		// set all parameters of FileHandle to null
		ofh.FilePath = null;
		ofh.ChunkServerStatus = null;
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals AppendRecord(FileHandle ofh, RID RecordID, int size) {
		// cannot write a record that is bigger than chunk's size
		if (size > ChunkServer.ChunkSize) {
			System.out.println("Record is larger than chunk size. Cannot AppendRecord.");
			return FSReturnVals.Fail;
		}
		
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		// get file metadata
		FileMD fmd = (FileMD) tempNode.GetData();
		
		
		// Case 1: File has never been written to, or is empty
		if (fmd.RecordIDInfo.isEmpty()) {
			
			// create new RID
			RID newRid = new RID();
			ChunkServer tempcs = ClientFS.chunkserver1;
			newRid.chunkhandle = ClientFS.chunkserver1.createChunk(); // TODO CHANGE LATER
			newRid.byteoffset = 0;
			newRid.size = size;
			
			// create new CSW (ChunkServerWritten)
			for (int i=0; i < ChunkServerCount; i++) {
				fmd.ChunkServerWritten.add(new ArrayList<Boolean>());
			}
			
			// update metadata
			fmd.RecordIDInfo.add(newRid);
			for (int i=0; i < ChunkServerCount; i++) {
				fmd.ChunkServerWritten.get(i).add(true); // TODO CHANGE LATER
				// When CS is networked with master,
				// this value will start as UNWRITTEN
				// and CS will use heartbeat to
				// set it as WRITTEN
			}
			
			
			// update FH and RID
			ofh.ChunkServerStatus = new int[ChunkServerCount];
			for (int i=0; i < ChunkServerCount; i++) {
				if (ChunkServerAvailability[i] == true) {
					ofh.ChunkServerStatus[i] = WRITTEN; // TODO CHANGE LATER
													// When CS is networked with master,
													// this value will start as UNWRITTEN
													// and CS will use heartbeat to
													// set it as WRITTEN
				}
				else {
					ofh.ChunkServerStatus[i] = DOWN;
				}
			}
			RecordID.chunkhandle = newRid.chunkhandle;
			RecordID.byteoffset = newRid.byteoffset;
			RecordID.size = size;
			
		}
		
		// Case 2: File has been written to, or is not empty
		else {
			
			// calculate space left in current chunk
			RID tailRid = fmd.RecordIDInfo.getLast();
			int spaceleft = ChunkServer.ChunkSize - tailRid.byteoffset - tailRid.size;
			

			// Case 2a: Chunk does not have enough room (must create new chunk)
			if (size > spaceleft) {
				
				// create new RID
				RID newRid = new RID();
				newRid.chunkhandle = ClientFS.chunkserver1.createChunk(); // TODO CHANGE LATER
				newRid.byteoffset = 0;
				newRid.size = size;
				
				// update metadata
				fmd.RecordIDInfo.add(newRid);
				for (int i=0; i < ChunkServerCount; i++) {
					fmd.ChunkServerWritten.get(i).add(true); // TODO CHANGE LATER
					// When CS is networked with master,
					// this value will start as UNWRITTEN
					// and CS will use heartbeat to
					// set it as WRITTEN
				}
				
				// update FH and RID
				ofh.ChunkServerStatus = new int[ChunkServerCount];
				for (int i=0; i < ChunkServerCount; i++) {
					if (ChunkServerAvailability[i] == true) {
						ofh.ChunkServerStatus[i] = WRITTEN; // TODO CHANGE LATER
						// When CS is networked with master,
						// this value will start as UNWRITTEN
						// and CS will use heartbeat to
						// set it as WRITTEN
					}
					else {
						ofh.ChunkServerStatus[i] = DOWN;
					}
				}
				RecordID.chunkhandle = newRid.chunkhandle;
				RecordID.byteoffset = newRid.byteoffset;
				RecordID.size = size;
				
			}
			
			// Case 2b: Chunk still has enough room for the record
			else {
				
				// create new RID
				RID newRid = new RID();
				newRid.chunkhandle = tailRid.chunkhandle;
				newRid.byteoffset = tailRid.byteoffset + tailRid.size;
				newRid.size = size;
				
				// update metadata
				fmd.RecordIDInfo.add(newRid);
				for (int i=0; i < ChunkServerCount; i++) {
					fmd.ChunkServerWritten.get(i).add(true); // TODO CHANGE LATER
					// When CS is networked with master,
					// this value will start as UNWRITTEN
					// and CS will use heartbeat to
					// set it as WRITTEN
				}
				
				// update FH and RID
				ofh.ChunkServerStatus = new int[ChunkServerCount];
				for (int i=0; i < ChunkServerCount; i++) {
					if (ChunkServerAvailability[i] == true) {
						ofh.ChunkServerStatus[i] = WRITTEN; // TODO CHANGE LATER
						// When CS is networked with master,
						// this value will start as UNWRITTEN
						// and CS will use heartbeat to
						// set it as WRITTEN
					}
					else {
						ofh.ChunkServerStatus[i] = DOWN;
					}
				}
				RecordID.chunkhandle = newRid.chunkhandle;
				RecordID.byteoffset = newRid.byteoffset;
				RecordID.size = size;
			}
			
		}
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		// get file metadata
		FileMD fmd = (FileMD) tempNode.GetData();
		
		// if file is empty, return error
		if (fmd.RecordIDInfo.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		// find index
		int removeIndex = IndexOf(fmd.RecordIDInfo, RecordID);
		
		// if this record does not exist, return error
		if (removeIndex == -1) {
			return FSReturnVals.Fail;
		}
		
		// remove this index
		fmd.RecordIDInfo.remove(removeIndex);
		for (int i=0; i < ChunkServerCount; i++) {
			fmd.ChunkServerWritten.get(i).remove(removeIndex);
		}
		
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadFirstRecord(FileHandle ofh, RID RecordID) {
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		// get file metadata
		FileMD fmd = (FileMD) tempNode.GetData();
		
		// if file is empty, return error
		if (fmd.RecordIDInfo.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		// get first record from first chunk
		RID firstRid = fmd.RecordIDInfo.peekFirst();
		
		// update FH and RID
		ofh.ChunkServerStatus = new int[ChunkServerCount];
		for (int i=0; i < ChunkServerCount; i++) {
			if (ChunkServerAvailability[i] == true) {
				if (fmd.ChunkServerWritten.get(i).get(0) == true) {
					ofh.ChunkServerStatus[i] = WRITTEN;
				}
				else {
					ofh.ChunkServerStatus[i] = UNWRITTEN;
				}
			}
			else {
				ofh.ChunkServerStatus[i] = DOWN;
			}
		}
		RecordID.chunkhandle = firstRid.chunkhandle;
		RecordID.byteoffset = firstRid.byteoffset;
		RecordID.size = firstRid.size;
		

		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadLastRecord(FileHandle ofh, RID RecordID) {
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		// get file metadata
		FileMD fmd = (FileMD) tempNode.GetData();
		
		// if file is empty, return error
		if (fmd.RecordIDInfo.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		// get first record from first chunk
		RID lastRid = fmd.RecordIDInfo.peekLast();
		
		// update FH and RID
		ofh.ChunkServerStatus = new int[ChunkServerCount];
		for (int i=0; i < ChunkServerCount; i++) {
			if (ChunkServerAvailability[i] == true) {
				if (fmd.ChunkServerWritten.get(i).get(fmd.RecordIDInfo.size()-1) == true) {
					ofh.ChunkServerStatus[i] = WRITTEN;
				}
				else {
					ofh.ChunkServerStatus[i] = UNWRITTEN;
				}
			}
			else {
				ofh.ChunkServerStatus[i] = DOWN;
			}
		}
		RecordID.chunkhandle = lastRid.chunkhandle;
		RecordID.byteoffset = lastRid.byteoffset;
		RecordID.size = lastRid.size;
		

		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, RID RecordID) {
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		// get file metadata
		FileMD fmd = (FileMD) tempNode.GetData();
		
		// if file is empty, return error
		if (fmd.RecordIDInfo == null) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		// check if this record is the last one
		RID lastRid = fmd.RecordIDInfo.getLast();
		if (pivot.equals(lastRid)) {
			RecordID = null;
			return FSReturnVals.Fail;
		}
		
		// get next record
		int pivIndex = IndexOf(fmd.RecordIDInfo, pivot);
		RID nextRid = fmd.RecordIDInfo.get(pivIndex+1);
		
		// update FH and RID
		ofh.ChunkServerStatus = new int[ChunkServerCount];
		for (int i=0; i < ChunkServerCount; i++) {
			if (ChunkServerAvailability[i] == true) {
				if (fmd.ChunkServerWritten.get(i).get(pivIndex+1) == true) {
					ofh.ChunkServerStatus[i] = WRITTEN;
				}
				else {
					ofh.ChunkServerStatus[i] = UNWRITTEN;
				}
			}
			else {
				ofh.ChunkServerStatus[i] = DOWN;
			}
		}
		RecordID.chunkhandle = nextRid.chunkhandle;
		RecordID.byteoffset = nextRid.byteoffset;
		RecordID.size = nextRid.size;
		
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, RID RecordID) {
		// if invalid filepath, return error
		Node tempNode = GetNode(ofh.FilePath);
		if (tempNode == null) {
			return FSReturnVals.FileDoesNotExist;
		}
		
		// get file metadata
		FileMD fmd = (FileMD) tempNode.GetData();
		
		// if file is empty, return error
		if (fmd.RecordIDInfo == null) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		// check if this record is the last one
		RID firstRid = fmd.RecordIDInfo.peekFirst();
		if (pivot.equals(firstRid)) {
			RecordID = null;
			return FSReturnVals.Fail;
		}
		
		// get next record
		int pivIndex = IndexOf(fmd.RecordIDInfo, pivot);
		RID nextRid = fmd.RecordIDInfo.get(pivIndex-1);
		
		// update FH and RID
		ofh.ChunkServerStatus = new int[ChunkServerCount];
		for (int i=0; i < ChunkServerCount; i++) {
			if (ChunkServerAvailability[i] == true) {
				if (fmd.ChunkServerWritten.get(i).get(pivIndex-1) == true) {
					ofh.ChunkServerStatus[i] = WRITTEN;
				}
				else {
					ofh.ChunkServerStatus[i] = UNWRITTEN;
				}
			}
			else {
				ofh.ChunkServerStatus[i] = DOWN;
			}
		}
		RecordID.chunkhandle = nextRid.chunkhandle;
		RecordID.byteoffset = nextRid.byteoffset;
		RecordID.size = nextRid.size;
		
		
		return FSReturnVals.Success;
	}
	
	
	// open a shell to process terminal directory commands
	public void CommandLine() {
		Scanner scan = new Scanner(System.in);
		String input;
		String [] args;
		
		
		// supports the following commands:
		// mkdir <filename>
		// rmdir <filename>
		// mv <filename>
		// ls
		// quit
		
		while (true) {
			// input
			System.out.print(">>> ");
			input = scan.nextLine();
			
			args = input.split(" ");
			
			if (args[0].equals("ls")) {
				String[] dirs = ListDir("/");
				for (int i=0; i < dirs.length; i++) {
					System.out.println(dirs[i]);
				}
				
				System.out.println("Used ls command");
			}
			else if (args[0].equals("mkdir")) {
				CreateDir("/", args[1]);
				
				System.out.println("Used mkdir command with " + args[1] + " argument");
			}
			else if (args[0].equals("rmdir")) {
				System.out.println("Used rmdir command with " + args[1] + " argument");
			}
			else if (args[0].equals("mv")) {
				System.out.println("Used mv command with " + args[1] + " argument");
			}
			else if (args[0].equals("quit")) {
				System.out.println("Exiting");
				break;
			}
			else if (args[0].equals("parsepath")) {
				ArrayList<String> parsedpath = ParsePath(args[1]);
				for (int i=0; i < parsedpath.size(); i++) {
					System.out.println(parsedpath.get(i));
				}
			}
			else {
				System.out.println("Invalid command...");
			}
			
		}
	}
	
	// process client requests through socket programming
	public void ReadAndProcessClientRequests()
	{
		
		//Used for communication with the Client via the network
		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
		ServerSocket commChanel = null;
//		DataOutputStream WriteOutput = null;
//		DataInputStream ReadInput = null;
//		ObjectOutputStream ObjectWriteOutput = null;
//		ObjectInputStream ObjectReadInput = null;
		ObjectOutputStream WriteOutput = null;
		ObjectInputStream ReadInput = null;
		
		try {
			//Allocate a port and write it to the config file for the Client to consume
			commChanel = new ServerSocket(ServerPort);
			ServerPort=commChanel.getLocalPort();
			PrintWriter outWrite=new PrintWriter(new FileOutputStream(MasterClientConfigFile));
			outWrite.println("localhost:"+ServerPort);
			outWrite.close();
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}
		
//		boolean done = false;
		Socket ClientConnection = null;  //A client's connection to the server
		
		String src, dirname, NewName, tgtdir, filename, FilePath;
		FileHandle ofh;
		RID RecordID, pivot;
		int size;
		ClientFS.FSReturnVals retval;
		String converted;

		while (true){
			try {
				System.out.println("Waiting on incoming client connections...");
				ClientConnection = commChanel.accept();
//				ReadInput = new DataInputStream(ClientConnection.getInputStream());
//				WriteOutput = new DataOutputStream(ClientConnection.getOutputStream());
//				ObjectReadInput = new ObjectInputStream(ClientConnection.getInputStream());
//				ObjectWriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				
				//Use the existing input and output stream as long as the client is connected
				while (!ClientConnection.isClosed()) {
					int CMD = ReadInput.readInt();
					switch (CMD){
					case CreateDirCMD:
						// read from client
						src = ReadInput.readUTF();
						dirname = ReadInput.readUTF();
						
						// execute on master
						retval = this.CreateDir(src, dirname);
						
						// return to client
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						break;
					
					case ListDirCMD:
						// read from client
						String tgt = ReadInput.readUTF();
						
						// execute on master
						String [] retlist = this.ListDir(tgt);
						
						// return to client
						int lsSize = retlist.length;
						WriteOutput.writeInt(lsSize);
						for (int i=0; i < lsSize; i++) {
							WriteOutput.writeUTF(retlist[i]);
						}
						WriteOutput.flush();
						
						break;
					
					case DeleteDirCMD:
						// read from client
						src = ReadInput.readUTF();
						dirname = ReadInput.readUTF();
						
						// execute on master
						retval = this.DeleteDir(src, dirname);

						// return to client
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						break;

					case RenameDirCMD:
						// read from client
						src = ReadInput.readUTF();
						NewName = ReadInput.readUTF();
						
						// execute on master
						retval = this.RenameDir(src, NewName);

						// return to client
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						break;

					case CreateFileCMD:
						

						if (ClientFS.chunkserver1 == null) {
							System.out.println("cs is null in Master's CreateFile");
						}
						
						
						// read from client
						tgtdir = ReadInput.readUTF();
						filename = ReadInput.readUTF();
						
						// execute on master
						retval = this.CreateFile(tgtdir, filename);

						// return to client
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						break;

					case DeleteFileCMD:
						// read from client
						tgtdir = ReadInput.readUTF();
						filename = ReadInput.readUTF();
						
						// execute on master
						retval = this.DeleteFile(tgtdir, filename);

						// return to client
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						break;
					
					case OpenFileCMD:
						// read from client
						FilePath = ReadInput.readUTF();
						ofh = (FileHandle) ReadInput.readObject();
						
						// execute on master
						retval = this.OpenFile(FilePath, ofh);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
					
					case CloseFileCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						
						// execute on master
						retval = this.CloseFile(ofh);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
					
					case AppendRecordCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						RecordID = (RID) ReadInput.readObject();
						size = ReadInput.readInt();
						
						// execute on master
						retval = this.AppendRecord(ofh, RecordID, size);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						WriteOutput.writeObject(RecordID);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
						
					case DeleteRecordCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						RecordID = (RID) ReadInput.readObject();
						
						// execute on master
						retval = this.DeleteRecord(ofh, RecordID);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						WriteOutput.writeObject(RecordID);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
						
					case ReadFirstRecordCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						RecordID = (RID) ReadInput.readObject();
						
						// execute on master
						retval = this.ReadFirstRecord(ofh, RecordID);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						WriteOutput.writeObject(RecordID);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
						
					case ReadLastRecordCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						RecordID = (RID) ReadInput.readObject();
						
						// execute on master
						retval = this.ReadLastRecord(ofh, RecordID);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						WriteOutput.writeObject(RecordID);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
						
					case ReadNextRecordCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						pivot = (RID) ReadInput.readObject();
						RecordID = (RID) ReadInput.readObject();
						
						// execute on master
						retval = this.ReadNextRecord(ofh, pivot, RecordID);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						WriteOutput.writeObject(RecordID);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
						
					case ReadPrevRecordCMD:
						// read from client
						ofh = (FileHandle) ReadInput.readObject();
						pivot = (RID) ReadInput.readObject();
						RecordID = (RID) ReadInput.readObject();
						
						// execute on master
						retval = this.ReadPrevRecord(ofh, pivot, RecordID);
						
						// return to client
						WriteOutput.writeObject(ofh);
						WriteOutput.flush();
						WriteOutput.writeObject(RecordID);
						WriteOutput.flush();
						
						converted = retval.name();
						WriteOutput.writeUTF(converted);
						WriteOutput.flush();
						
						
						break;
						
						
					default:
						System.out.println("Error in Master, specified CMD "+CMD+" is not recognized.");
						break;
					}
				}
			} catch (IOException ex){
				System.out.println("ClientFS Disconnected");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
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
	
	public void ChunkServerConnect() {
		
		
		while (ChunkServerCount != ChunkServerExpected) {
			
			int ServerPort = 0; // automatically ask OS to find open port
			
			try {
				// Open connection up to ChunkServers
				csCommChannel = new ServerSocket(ServerPort);
				ServerPort = csCommChannel.getLocalPort();
				PrintWriter outWrite=new PrintWriter(new FileOutputStream(ChunkServer.MasterChunkServerConfigFile));
				outWrite.println("localhost:"+ServerPort);
				outWrite.close();
				
				
				// ChunkServer connects to Master
				csConnection = csCommChannel.accept();
				WriteOutputToCS = new ObjectOutputStream(csConnection.getOutputStream());
				ReadInputFromCS = new ObjectInputStream(csConnection.getInputStream());
				
				
			} catch (IOException e) {
				System.out.println("Error, failed to open a new socket to listen on.");
				e.printStackTrace();
			}
			
			
		}
		
	}
	
	/*
	 * 
	 * UTILITY FUNCTIONS
	 * 
	 */
	
	
	/* given a path, returns a list of each directory
	 * For example, path = "/Jerry/Documents/File1"
	 * will return: { "", "Jerry", "Documents", "File1" }
	 * For example, path = "/Jerry/Documents/Homework/"
	 * will return: { "", "Jerry", "Documents", "Homework" }
	*/
	protected static ArrayList<String> ParsePath(String path) {
		ArrayList<String> directories = new ArrayList<String>();
		
		int leftbound = 0, rightbound = 0;
		for (int i=0; i < path.length(); i++) {
			if (path.charAt(i) == '/') {
				rightbound = i;
				directories.add(path.substring(leftbound, rightbound));
				leftbound = i+1;
			}
		}
		
		if (rightbound != path.length()-1) {
			directories.add(path.substring(leftbound));
		}
		
		
		return directories;
	}
	
	// returns a node
	// or null if that node doesn't exist
	protected static Node GetNode(String filepath) {
		Node currentNode = namespace.root;
		
		ArrayList<String> path = ParsePath(filepath);
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("")) {
			return null;
		}
		
		// find the proper parent node
		// if the path is invalid, returns error
		String nextdir;
		for (int i=1; i < path.size(); i++) {
			nextdir = path.get(i);
			currentNode = currentNode.GetChild(nextdir);
			if (currentNode == null) {
				return null;
			}
		}
		
		return currentNode;
	}
	
	
	// this method is here because Java is stupid
	// RAGE RAGE RAGE
	int IndexOf(LinkedList<RID> myll, RID myrid) {
		int index = -1;
		RID tempRid;
		for (int i=0; i < myll.size(); i++) {
			tempRid = myll.get(i);
			if (tempRid.byteoffset == myrid.byteoffset
					&& tempRid.chunkhandle.equals(myrid.chunkhandle)
					&& tempRid.size == myrid.size) {
				return i;
			}
		}
		return index;
	}
	
	
	public static void main(String [] args) {
		Master ms = new Master();
//		ms.CommandLine();
//		ms.ChunkServerConnect();
		ms.ReadAndProcessClientRequests();
	}
}
