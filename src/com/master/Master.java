package com.master;

import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;

import java.util.Scanner;

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
				System.out.println("Used ls command");
			}
			else if (args[0].equals("mkdir")) {
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
			else {
				System.out.println("Invalid command...");
			}
			
		}
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
