package org.janusgraph.example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class DedupeProperty {
private static BufferedReader br;

public static void main(String args[]) throws IOException {
	String[] files = { 
	    				"/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToPropertyName.csv",
	    				"/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToPropertyNameRemaining.csv",
	    				"/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToVertexLabel.csv",
						"/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToEdgeLabel.csv",
					};
	HashMap<String, Integer> properties = new HashMap<>();
	HashMap<String, Integer> labels = new HashMap<>();
	HashSet<String> contents = new HashSet<>();
	
	for(int i=0; i<2; i++) {
		br = new BufferedReader(new FileReader(files[i]));
		String line = br.readLine();
		while(line != null) {
			if(!line.contains("$")) {
				contents.add(line);
			}
			line = br.readLine();
		}
		br.close();
	}
	
	System.out.println(contents.size());
	for(String s : contents) {
		String[] values = s.split(",");
		properties.put(values[1], Integer.valueOf(values[0]));
		System.out.println("{\""+values[1]+"\","+values[0]+"},");
	}
	
	contents.clear();
	
	for(int i=2; i<4; i++) {
		br = new BufferedReader(new FileReader(files[i]));
		String line = br.readLine();
		while(line != null) {
			if(!(line.contains("v[") ||line.contains("$") )) {
				contents.add(line);
			}
			line = br.readLine();
		}
		br.close();
	}
	
	System.out.println(contents.size());
	for(String s : contents) {
		String[] values = s.split(",");
		properties.put(values[1], Integer.valueOf(values[0]));
		System.out.println("{\""+values[1]+"\","+values[0]+"},");
	}
    
	System.out.println(contents.size());
	for(String s : contents) {
		String[] values = s.split(",");
		properties.put(values[1], Integer.valueOf(values[0]));
		System.out.println("{"+values[0]+",\""+values[1]+"\"},");
	}
}

}
