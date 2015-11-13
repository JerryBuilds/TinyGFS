package com.master;

import com.client.*;

import java.util.*;

public class FileMD extends Metadata {
//	public ArrayList<ArrayList<RID>> cs1info;
	public LinkedList<RID> RecordIDInfo;
	public ArrayList<ArrayList<Boolean>> ChunkServerWritten;
	public FileMD() {
		RecordIDInfo = new LinkedList<RID>();
		ChunkServerWritten = new ArrayList<ArrayList<Boolean>>();
	}
}
