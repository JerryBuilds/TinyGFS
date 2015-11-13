package com.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

import com.client.ClientFS.FSReturnVals;
import com.client.FileHandle;

public class Master {
	
	Tree namespace;
	
	public Master() {
		// initialize all data structure
		
		Directory rootdir = new Directory();
		rootdir.name = "";
		namespace = new Tree(rootdir);
		
	}
	
	public FSReturnVals CreateDir(String src, String dirname) {
		Node parentNode = namespace.root;
		
		ArrayList<String> path = ParsePath(src);
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("/")) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		Directory newDirectory = new Directory();
		newDirectory.name = dirname;
		
		// find the proper parent node
		// if the path is invalid, returns error
		String currentdir;
		for (int i=1; i < path.size(); i++) {
			currentdir = path.get(i).substring(0, path.get(i).length()-1);
			parentNode = parentNode.GetChild(currentdir);
			if (parentNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
		}
		
		// checks if directory already exists
		// if so, then return error
		currentdir = dirname;
		if (parentNode.GetChild(currentdir) != null) {
			return FSReturnVals.DestDirExists;
		}
		
		// add the new directory
		parentNode.AddChild(newDirectory);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals DeleteDir(String src, String dirname) {
		Node parentNode = namespace.root;
		
		ArrayList<String> path = ParsePath(src);
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("/")) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		// find the proper parent node
		// if the path is invalid, returns error
		String currentdir;
		for (int i=1; i < path.size(); i++) {
			currentdir = path.get(i).substring(0, path.get(i).length()-1);
			parentNode = parentNode.GetChild(currentdir);
			if (parentNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
		}
		
		// if directory is not empty, CANNOT DELETE!!
		ArrayList<Node> children = parentNode.GetChild(dirname).GetChildren();
		if (!children.isEmpty()) {
			return FSReturnVals.DirNotEmpty;
		}
		
		// remove the directory
		parentNode.RemoveChild(dirname);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals RenameDir(String src, String NewName) {
		Node currentNode = namespace.root;
		
		ArrayList<String> path = ParsePath(src);
		ArrayList<String> newpath = ParsePath(NewName);
		
		// check if both give the same number of levels and are all same up to the last one
		if (path.size() != newpath.size()) {
			return FSReturnVals.Fail;
		}
		for (int i=0; i < path.size()-1; i++) {
			if (!path.get(i).equals(newpath.get(i))) {
				return FSReturnVals.Fail;
			}
		}
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("/")) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		// find the proper parent node
		// if the path is invalid, returns error
		String currentdir = "";
		for (int i=1; i < path.size(); i++) {
			currentdir = path.get(i);
			if (i != path.size()-1) {
				currentdir = currentdir.substring(0, currentdir.length()-1);
			}
			currentNode = currentNode.GetChild(currentdir);
			if (currentNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
		}
		
		// rename the current directory
		currentNode.SetName(newpath.get(newpath.size()-1));
		
		return FSReturnVals.Success;
	}
	
	public String[] ListDir(String tgt) {
		Node parentNode = namespace.root;
		
		// make sure the passed-in directory has a '/' at end
		if (tgt.charAt(tgt.length() - 1) != '/') {
			tgt += '/';
		}
		
		ArrayList<String> path = ParsePath(tgt);
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("/")) {
			return null;
		}
		
		// find the proper parent node
		// if the path is invalid, returns error
		for (int i=1; i < path.size(); i++) {
			parentNode = parentNode.GetChild(path.get(i).substring(0, path.get(i).length()-1));
			if (parentNode == null) {
				return null;
			}
		}
		
		ArrayList<Node> allDescendants = parentNode.GetAllDescendants();
		
		String[] ls = new String[allDescendants.size()];
		for (int i=0; i < ls.length; i++) {
			ls[i] = allDescendants.get(i).GetFullPath();
		}
		
		return ls;
	}
	
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		Node parentNode = namespace.root;
		
		ArrayList<String> path = ParsePath(tgtdir);
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("/")) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		File newFile = new File();
		newFile.name = filename;
		
		// find the proper parent node
		// if the path is invalid, returns error
		String currentdir;
		for (int i=1; i < path.size(); i++) {
			currentdir = path.get(i).substring(0, path.get(i).length()-1);
			parentNode = parentNode.GetChild(currentdir);
			if (parentNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
		}
		
		// checks if directory already exists
		// if so, then return error
		currentdir = filename;
		if (parentNode.GetChild(currentdir) != null) {
			return FSReturnVals.DestDirExists;
		}
		
		// add the new directory
		parentNode.AddChild(newFile);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		Node parentNode = namespace.root;
		
		ArrayList<String> path = ParsePath(tgtdir);
		
		// if the first directory is not root, returns error
		if (!path.get(0).equals("/")) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		// find the proper parent node
		// if the path is invalid, returns error
		String currentdir;
		for (int i=1; i < path.size(); i++) {
			currentdir = path.get(i).substring(0, path.get(i).length()-1);
			parentNode = parentNode.GetChild(currentdir);
			if (parentNode == null) {
				return FSReturnVals.SrcDirNotExistent;
			}
		}
		
		// if directory is not empty, CANNOT DELETE!!
		ArrayList<Node> children = parentNode.GetChild(filename).GetChildren();
		if (!children.isEmpty()) {
			return FSReturnVals.DirNotEmpty;
		}
		
		// remove the directory
		parentNode.RemoveChild(filename);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		return null;
	}
	
	public FSReturnVals CloseFile(FileHandle ofh) {
		return null;
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
	public static void ReadAndProcessRequests() {
		Master ms = new Master();
		ServerSocket commChannel = null;
		ObjectOutputStream WriteOutput = null;
		ObjectInputStream ReadInput = null;
		PrintStream PS = null;
		Socket ClientConnection = null;
		BufferedReader BR = null;
		
		try {
			commChannel = new ServerSocket(1240);
			
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		boolean done = false;
		
		while (!done) {
			try  {
				ClientConnection = commChannel.accept();

				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				PS = new PrintStream(ClientConnection.getOutputStream());
				BR = new BufferedReader(
			            new InputStreamReader(ClientConnection.getInputStream()));
				
				while (!ClientConnection.isClosed()) {
					String cmd = BR.readLine();
					if (cmd == "CreateDir") {
						String source = BR.readLine();
						String dir = BR.readLine();
						PS.println(ms.CreateDir(source, dir));
					}
					if (cmd == "DeleteDir") {
						String source = BR.readLine();
						String dir = BR.readLine();
						PS.println(ms.CreateDir(source,  dir));
					}
					if (cmd == "RenameDir") {
						String source = BR.readLine();
						String name = BR.readLine();
						PS.println(ms.CreateDir(source, name));
					}
					if (cmd == "ListDir") {
						String tgt = BR.readLine();
						WriteOutput.writeObject(ms.ListDir(tgt));
						
					}
				}
				PS.flush();

				
			}
			catch (IOException ex) {
				ex.printStackTrace();
			}
			
		}
		
	}
	
	// given a path, returns a list of each directory
	public ArrayList<String> ParsePath(String path) {
		ArrayList<String> directories = new ArrayList<String>();
		
		int leftbound = 0, rightbound = 0;
		for (int i=0; i < path.length(); i++) {
			if (path.charAt(i) == '/') {
				rightbound = i+1;
				directories.add(path.substring(leftbound, rightbound));
				leftbound = i+1;
			}
		}
		
		if (rightbound != path.length()) {
			directories.add(path.substring(leftbound));
		}
		
		
		return directories;
	}
	
	public static void main(String [] args) {
		ReadAndProcessRequests();
	}
}
