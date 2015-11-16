package com.master;

import java.util.Scanner;

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
	public void Redo(String op){

		switch (action){
		case CreateDir:
			System.out.println("Trasaction CreateDir Redo");
			//TODO: Do CreateDir
			break;
		case DeleteDir:
			System.out.println("Trasaction DeleteDir Redo");
			//TODO: Do DeleteDir
			break;
		case RenameDir:
			System.out.println("Trasaction RenameDir Redo");
			//TODO: Do RenameDir
			break;
		case CreateFile:
			System.out.println("Trasaction CreateFile Redo");
			//TODO: Do CreateFile
			break;
		case DeleteFile:
			System.out.println("Trasaction DeleteFile Redo");
			//TODO: Do DeleteFile
			break;
		default:
			System.out.println("Trasaction type not recognized");
			break;
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
