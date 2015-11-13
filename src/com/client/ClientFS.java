package com.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;

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
		Fail //Returned when a method fails
	}
	public static Master master;
	public static ChunkServer chunkserver1;
	public static Client client;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	static BufferedReader BR;
	static BufferedWriter BW;
	static PrintStream PS;
	public ClientFS() {;
			try {
				ClientSocket = new Socket("localhost", 1240);
				WriteOutput = new ObjectOutputStream(ClientSocket.getOutputStream());
				ReadInput = new ObjectInputStream(ClientSocket.getInputStream());
				BR = new BufferedReader(
			            new InputStreamReader(ClientSocket.getInputStream()));
				PrintStream PS = new PrintStream(ClientSocket.getOutputStream());
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				System.out.println("Couldn't create a socket");
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("IOException with creating a socket");
				e.printStackTrace();
			}

	
		
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		try {
		


	
		PS.println("CreateDir");
		PS.println(src);
		PS.println(dirname);
		PS.flush();
		FSReturnVals data = FSReturnVals.valueOf(BR.readLine());
		return data;
		 
	}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		try {
			PS.println("DeleteDir");
			PS.println(src);
			PS.println(dirname);
			PS.flush();
			FSReturnVals data = FSReturnVals.valueOf(BR.readLine());
			return data;
			 
		}
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			
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
			PS.println("RenameDir");
			PS.println(src);
			PS.println(NewName);
			PS.flush();
			FSReturnVals data = FSReturnVals.valueOf(BR.readLine());
			return data;
			 
		}
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
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
			PS.println("ListDir");
			PS.println(tgt);
			PS.flush();
			String[] myObjects;
			try {
				myObjects = (String[])ReadInput.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			return myObjects;
			 
		}
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		return master.CreateFile(tgtdir, filename);
		//return null;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		return master.DeleteFile(tgtdir, filename);
		//return null;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx")
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		return null;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		return null;
	}

}
