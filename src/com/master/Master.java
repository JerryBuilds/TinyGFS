package com.master;

import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;

public class Master {
	
	public Master() {
		// initialize all data structure
		
		
	}
	
	public FSReturnVals CreateDir(String src, String dirname) {
		return null;
	}
	
	public FSReturnVals DeleteDir(String src, String dirname) {
		return null;
	}
	
	public FSReturnVals RenameDir(String src, String NewName) {
		return null;
	}
	
	public String[] ListDir(String tgt) {
		return null;
	}
	
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		return null;
	}
	
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		return null;
	}
	
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		return null;
	}
	
	public FSReturnVals CloseFile(FileHandle ofh) {
		return null;
	}
	
	
	public static void CommandLine() {
		// open a shell to process terminal directory commands
	}
	
	
	public void ReadAndProcessRequests() {
		// process client requests through networking
		// socket programming
		
		
	}
	
	public static void main(String [] args) {
		Master ms = new Master();
		CommandLine();
	}
}
