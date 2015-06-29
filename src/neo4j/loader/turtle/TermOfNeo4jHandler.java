package neo4j.loader.turtle;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import taobe.tec.jcc.JChineseConvertor;

public class TermOfNeo4jHandler implements RDFHandler {

	private int totalNodes = 0;
	private int sinceLastCommit = 0;

	private long tick = System.currentTimeMillis();
	private GraphDatabaseService db;
	private Index<Node> index;

	private Transaction tx;

	public TermOfNeo4jHandler(GraphDatabaseService db) {
		this.db = db;
		index = db.index().forNodes("NodeIndex");
		tx = db.beginTx();		
	}

	@Override
	public void handleStatement(Statement st) {
		try {
			
			
			JChineseConvertor jChineseConvertor = JChineseConvertor  .getInstance();  
			
			Resource subject = st.getSubject();
			URI predicate = st.getPredicate();			
			String predicateName = predicate.getLocalName();					
			Value object = st.getObject();

			
			if (!(subject.stringValue().indexOf("/Template:")>0)){
				String lemma;
				if (subject.stringValue().indexOf("Category")>0)
					lemma=jChineseConvertor.s2t(subject.stringValue().substring(subject.stringValue().lastIndexOf("/Category:")+10));
				else
					lemma=jChineseConvertor.s2t(subject.stringValue().substring(subject.stringValue().lastIndexOf("/resource/")+10));
				
				// Check index for subject			
				//System.out.println("-----"+lemma);
				Node subjectNode;
				IndexHits<Node> hits = index.get("Lemma",lemma);
				if (hits.hasNext()) { // node exists
					subjectNode = hits.next();
				} else {
					subjectNode = db.createNode();								
					subjectNode.setProperty("Lemma",  lemma);
					if (subject.stringValue().indexOf("Category")>0)
						subjectNode.setProperty("SimplifiedChinese",  subject.stringValue().substring(subject.stringValue().lastIndexOf("/Category:")+10));
					else
						subjectNode.setProperty("SimplifiedChinese",  subject.stringValue().substring(subject.stringValue().lastIndexOf("/resource/")+10));
					index.add(subjectNode, "Lemma",lemma);
				}
			}
				
	
				totalNodes++;
	
				long nodeDelta = totalNodes - sinceLastCommit;
				long timeDelta = (System.currentTimeMillis() - tick) / 1000;
	
				//if (nodeDelta >= 115929 || timeDelta >= 5) { // Commit every 150k operations or every 30 seconds
				if (totalNodes==381048){	
					tx.success();
					tx.finish();
					tx = db.beginTx();
					sinceLastCommit = totalNodes;
				}
				System.out.println(totalNodes);
			
		} catch (Exception e) {
			e.printStackTrace();
			tx.finish();
			tx = db.beginTx();
		}
	}

	public int getCountedStatements() {
		return totalNodes;
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleComment(String arg0) throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void startRDF() throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

}
