/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.FileUtils;
public class ExportDictionary
{
    private static final String DB_PATH = "C:\\neo4j-db";
    GraphDatabaseService db;
    
    
    public static void main( final String[] args ) throws FileNotFoundException, UnsupportedEncodingException
    {
    	
    	PrintWriter writer = new PrintWriter("D:\\dic.txt", "UTF-8");
    	
    	GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        
        ExecutionEngine engine = new ExecutionEngine( db );
        ExecutionResult result = engine.execute( "start n=Node(*)where has(n.Lemma) and has(n.SimplifiedChinese) return distinct n.Lemma,n.SimplifiedChinese" );
        for ( Map<String, Object> row : result )
        {
        	String rows = "";int i=0;
            for ( Entry<String, Object> column : row.entrySet() )
            {
            	if (i==0)
            		rows +=column.getValue() + "|";
            	else
            		rows +=column.getValue();
            	i++;
            }
            //rows += "\n";
            System.out.println(rows);            
            writer.println(rows);            
            
        }
        writer.close();
        
    	
        db.shutdown();
    }
}