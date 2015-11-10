package com.master;

import com.client.FileHandle;
import com.master.Node;
import com.client.ClientFS.FSReturnVals;

import java.util.ArrayList;
import java.util.Scanner;

public class Master {
	
	Tree namespace;
	
	public Master() {
		// initialize all data structure
		
		Directory rootdir = new Directory();
		rootdir.name = "/";
		rootdir.fulldirpath = "/";
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
		newDirectory.fulldirpath = src + dirname;
		
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
		
		
		parentNode.RemoveChild(dirname);
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals RenameDir(String src, String NewName) {
		return null;
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
			ls[i] = allDescendants.get(i).GetData().fulldirpath;
		}
		
		return ls;
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
	public void ReadAndProcessRequests() {
		
		
		
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
		Master ms = new Master();
		ms.CommandLine();
	}
}
