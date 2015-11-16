package com.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;

import com.master.Transaction.Command;

public class Log {
	//each log contains a list of transactions
	String filename; //file name of the log file
	File logFile;
	FileInputStream fis;
	FileOutputStream fos;
	ArrayList<Transaction> transactions; //a list of transactions in this log file.
	
	public Log(String filename){
		transactions = new ArrayList<Transaction>();
		this.filename = filename;
		this.logFile = new File(filename);
		
		try {
			if(!logFile.exists()){
				this.logFile.createNewFile();	
			}
			this.fis = new FileInputStream(logFile);
			this.fos = new FileOutputStream(logFile, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void Commit(Transaction T){
		//generate a commit log record for the transaction in the log file
		//append the log form of that transaction.
		String msg = Integer.toString(transactions.size()) + ",Commit"; 
		AddMessage(msg);
		transactions.add(T);
	}
	
	public void Start(){
		//generate a start log 
		String msg = Integer.toString(transactions.size()) + ",Start"; 
		AddMessage(msg);
	}
	
	public void AddMessage(String msg){
		
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos));
		pw.println(msg);
		pw.flush();
		
	}
	
	//TODO: Parse log file and load transaction info when master starts.
	public void Load(){
		
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String msg = null;
			String line = br.readLine();
			while(line != null){
				Transaction t = new Transaction();
				msg = line+"\n";
				line = br.readLine();
				if(line != null){
					msg += line + "\n";
				}
				line = br.readLine();
				if(line != null){
					msg += line;
				}
				line = br.readLine();
//				System.out.println(msg);
				if(t.toTransaction(msg)){
					this.transactions.set(Integer.parseInt(t.ID), t);	
				}
			}
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
	}
	
	public static void main(String [] args) {
		Log l = new Log("log.txt");
		l.Start();
		Transaction t = new Transaction(Command.CreateDir, "./", "newDir", l.transactions.size());
		l.AddMessage(t.toString());
		l.Commit(t);
		l.Start();
		Transaction t1 = new Transaction(Command.CreateFile, "./", "newFile", l.transactions.size());
//		l.AddMessage(t1.toString());
//		l.Commit(t1);
//		
		l.Load();
	}
}
