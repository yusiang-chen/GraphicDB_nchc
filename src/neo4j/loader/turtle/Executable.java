package neo4j.loader.turtle;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.turtle.TurtleParser;

public class Executable {
	private static final String DB_PATH = "C:\\neo4j-db";
	private static GraphDatabaseService graphDb;
	public static void main(String[] args) throws Exception {

		File file = new File("C:\\cygwin\\home\\YuSiang\\page_links_zh.ttl\\4.ttl");
		
		if(!file.canRead())
			throw new Exception("Can't read the file.");
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		
		RDFParser rdfParser = new TurtleParser();
		
		
		//Choose a class, LiteralOfNeo4jHandler or  TermOfNeo4jHandler to parse a file
		LiteralOfNeo4jHandler handler = new LiteralOfNeo4jHandler(graphDb);
		rdfParser.setRDFHandler(handler);
		
		rdfParser.parse(bis, file.toURI().toString());
		
		System.out.println(handler.getCountedStatements());
		
		System.out.println( "Shutting down database ..." );
		graphDb.shutdown();
		
	}

}
