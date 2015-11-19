package com.master;

import java.util.ArrayList;
import java.util.Scanner;

import com.client.ClientFS.FSReturnVals;

public class Transaction {
	//public static int counter;
	public String ID;

	public String oldPath;
	public String newPath;
	public Command action;
	
	public Transaction(){
		
	}
	
	public Transaction(Command c, String OP, String NP, int num){
		//counter = num;
		//counter ++;
		this.ID = Integer.toString(num);
		this.action = c;
		this.oldPath = OP;
		this.newPath = NP;
//		System.out.println ( "Transaction" + ID + "Created for " + action);
		
		//TODO: each time the master restarts, the counter
		//should be the most recent ID + 1; ->is it neccessary if counter is static?
		
	}
	
	//Redo function is idempotent
	public FSReturnVals Redo(){
		// get node
		Node oldNode = Master.GetNode(oldPath);
		switch (action){
		case CreateDir:
			System.out.println("Trasaction CreateDir Redo");

			if (oldNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
			
			// check if directory already exists
			if (oldNode.GetChild(newPath) != null) {
				return FSReturnVals.DirExists;
			}
			
			// add directory
			DirectoryMD newDirectory = new DirectoryMD();
			newDirectory.name = newPath;
			oldNode.AddChild(newDirectory);
			
			return FSReturnVals.Success;
//			break;
		case DeleteDir:
			System.out.println("Trasaction DeleteDir Redo");

			if (oldNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
			
			// check if directory exists
			if (oldNode.GetChild(newPath) == null) {
				return FSReturnVals.DirDoesNotExist;
			}
			
			// if directory is not empty, CANNOT DELETE!
			if (!oldNode.GetChild(newPath).GetChildren().isEmpty()) {
				return FSReturnVals.DirNotEmpty;
			}
			
			// delete current directory
			oldNode.RemoveChild(newPath);
			
			return FSReturnVals.Success;
		case RenameDir:
			System.out.println("Trasaction RenameDir Redo");
			ArrayList<String> path = Master.ParsePath(oldPath);
			ArrayList<String> newpath = Master.ParsePath(newPath);
			if (path.size() != newpath.size()) {
				return FSReturnVals.Fail;
			}
			for (int i=0; i < path.size()-1; i++) {
				if (!path.get(i).equals(newpath.get(i))) {
					return FSReturnVals.Fail;
				}
			}
			
			if (oldNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
			
			// rename
			oldNode.SetName(newpath.get(newpath.size()-1));
			
			return FSReturnVals.Success;
			
		case CreateFile:
			System.out.println("Trasaction CreateFile Redo");
			if (oldNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
			
			// check if directory already exists
			if (oldNode.GetChild(newPath) != null) {
				return FSReturnVals.FileExists;
			}
			
			// add directory
			FileMD newFile = new FileMD();
			newFile.name = newPath;
			oldNode.AddChild(newFile);
			
			return FSReturnVals.Success;
		case DeleteFile:
			System.out.println("Trasaction DeleteFile Redo");
			
			
			// if directory does not exist, return error
			if (oldNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
			
			// if file does not exist, return error
			if (oldNode.GetChild(newPath) == null) {
				return FSReturnVals.FileDoesNotExist;
			}
			
			// delete the file
			oldNode.RemoveChild(newPath);
			
			return FSReturnVals.Success;
		default:
			System.out.println("Trasaction type not recognized");
			return FSReturnVals.Fail;
		}
	}
	
	//Format this transaction into log form.
	public String toString(){
		StringBuilder log = new StringBuilder(this.ID);
		log.append(",");
		log.append(action); log.append(",");
		log.append(oldPath); log.append(",");
		log.append(newPath); //log.append("\n");
		
//		System.out.println(log.toString());
		return log.toString();
	}
	
	public boolean toTransaction(String msg){
		String[] tokens = msg.split(",|\\\n");
//		for(int i = 0; i < tokens.length; ++i){
//			System.out.println(tokens[i]);
//		}
		if(tokens.length == 8){
			int index = 2;
			this.ID = tokens[index++];
			this.action = Command.valueOf(tokens[index++]);
			this.oldPath = tokens[index++];
			this.newPath = tokens[index++];
			
			return true;
		}
		else{
			//this transaction is not completely logged.
			System.out.println("This transaction "+ this.ID + " is not complete");
			return false;
		}
		
	}
	
	public enum Command{
		CreateDir, DeleteDir, RenameDir, CreateFile, DeleteFile
	}
	
}
