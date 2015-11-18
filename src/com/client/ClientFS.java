package com.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.chunkserver.ChunkServer;
import com.master.Master;

public class ClientFS {

	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail, //Returned when a method fails
		DirDoesNotExist
	}
	public static Master master;
//	public static ChunkServer chunkserver1 = new ChunkServer();
	public static Client client;
	
	public static int MasterPort = 0;
	public static Socket ClientMasterSocket;
//	private static DataOutputStream WriteOutput;
//	private static DataInputStream ReadInput; 
//	private static ObjectOutputStream ObjectWriteOutput;
//	private static ObjectInputStream ObjectReadInput;
	public static ObjectOutputStream WriteOutput;
	public static ObjectInputStream ReadInput;
	
	public ClientFS() {
		master = new Master();
//		chunkserver1 = new ChunkServer();
//		if (chunkserver1 == null) {
//			System.out.println("cs is null in ClientFS constructor");
//		}
		client = new Client();
		InitializeMasterConnection();
	}
	
	public void InitializeMasterConnection() {
		if (ClientMasterSocket != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(Master.MasterClientConfigFile));
			String port = binput.readLine();
			port = port.substring( port.indexOf(':')+1 );
			MasterPort = Integer.parseInt(port);
			
			ClientMasterSocket = new Socket("127.0.0.1", MasterPort);
			WriteOutput = new ObjectOutputStream(ClientMasterSocket.getOutputStream());
			ReadInput = new ObjectInputStream(ClientMasterSocket.getInputStream());
			
			
		}catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ Master.MasterClientConfigFile +" containing the port of the ChunkServer is missing.");
		}catch (IOException e) {
			e.printStackTrace();
			System.out.println("Can't find file.");
		}
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		try {
			// send command
			WriteOutput.writeInt(Master.CreateDirCMD);
			
			// send arguments
			WriteOutput.writeUTF(src);
			WriteOutput.writeUTF(dirname);
			WriteOutput.flush();
			
			// retrieve response
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
//		return master.CreateDir(src, dirname);
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		try {
			// send command
			WriteOutput.writeInt(Master.DeleteDirCMD);
			
			// send arguments
			WriteOutput.writeUTF(src);
			WriteOutput.writeUTF(dirname);
			WriteOutput.flush();

			// retrieve response
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
//		return master.DeleteDir(src, dirname);
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		try {
			// send command
			WriteOutput.writeInt(Master.RenameDirCMD);
			
			// send arguments
			WriteOutput.writeUTF(src);
			WriteOutput.writeUTF(NewName);
			WriteOutput.flush();

			// retrieve response
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
//		return master.RenameDir(src, NewName);
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		try {
			// send command
			WriteOutput.writeInt(Master.ListDirCMD);
			
			// send argument
			WriteOutput.writeUTF(tgt);
			WriteOutput.flush();
			
			// retrieve response
			int lsSize = ReadInput.readInt();
			String [] retval = new String[lsSize];
			for (int i=0; i < lsSize; i++) {
				retval[i] = ReadInput.readUTF();
			}
			
			return retval;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
//		return master.ListDir(tgt);
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		try {
			// send command
			WriteOutput.writeInt(Master.CreateFileCMD);
			
			// send arguments
			WriteOutput.writeUTF(tgtdir);
			WriteOutput.writeUTF(filename);
			WriteOutput.flush();

			// retrieve response
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
//		return master.CreateFile(tgtdir, filename);
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		try {
			// send command
			WriteOutput.writeInt(Master.DeleteFileCMD);
			
			// send arguments
			WriteOutput.writeUTF(tgtdir);
			WriteOutput.writeUTF(filename);
			WriteOutput.flush();

			// retrieve response
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
//		return master.DeleteFile(tgtdir, filename);
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx")
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		try {
			// send command
			WriteOutput.writeInt(Master.OpenFileCMD);
			
			// send arguments
			WriteOutput.writeUTF(FilePath);
			WriteOutput.writeObject(ofh);
			WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		
		return null;
//		return master.OpenFile(FilePath, ofh);
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		try {
			// send command
			WriteOutput.writeInt(Master.CloseFileCMD);
			
			// send arguments
			WriteOutput.writeObject(ofh);
			WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			String retval = ReadInput.readUTF();
			FSReturnVals converted = FSReturnVals.valueOf(retval);
			
			
			return converted;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		return null;
//		return master.CloseFile(ofh);
	}

}
