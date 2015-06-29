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

public class LiteralOfNeo4jHandler implements RDFHandler {

	private int totalNodes = 0;
	private int sinceLastCommit = 0;

	private long tick = System.currentTimeMillis();
	private GraphDatabaseService db;
	private Index<Node> index;

	private Transaction tx;

	public LiteralOfNeo4jHandler(GraphDatabaseService db) {
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

			// Check index for subject
			String lemma=jChineseConvertor.s2t(subject.stringValue().substring(subject.stringValue().lastIndexOf("/Category:")+10));
			//System.out.println("-----"+subject.stringValue());
			Node subjectNode;
			IndexHits<Node> hits = index.get("Lemma",lemma);
			if (hits.hasNext()) { // node exists
				subjectNode = hits.next();
			} else {
				subjectNode = db.createNode();
				//subjectNode.setProperty("__URI__",subject.stringValue());				
				subjectNode.setProperty("Lemma",  lemma);								
				index.add(subjectNode, "Lemma",lemma);
			}
			
			if (object instanceof Literal) {
				
				URI type = ((Literal) object).getDatatype();
				Object value;
				if (type == null) // treat as String
					value = object.stringValue();
				else {
					String localName = type.getLocalName();

					if (localName.toLowerCase().contains("integer") || localName.equals("long")) {
						value = ((Literal) object).longValue();
					} else if (localName.toLowerCase().contains("short")) {
						value = ((Literal) object).shortValue();
					} else if (localName.equals("byte")) {
						value = ((Literal) object).byteValue();
					} else if (localName.equals("char")) {
						value = ((Literal) object).byteValue();
					} else if (localName.equals("float")) {
						value = ((Literal) object).floatValue();
					} else if (localName.equals("double")) {
						value = ((Literal) object).doubleValue();
					} else if (localName.equals("boolean")) {
						value = ((Literal) object).booleanValue();
					} else {
						value = ((Literal) object).stringValue();
					}
				}
				
				
				lemma=jChineseConvertor.s2t(value.toString());
				Node objectNode;	
				hits = index.get("Lemma",lemma);
				if (hits.hasNext()) { // node exists
					objectNode = hits.next();
				} else {
					objectNode = db.createNode();
					objectNode.setProperty("Lemma", lemma);		
					objectNode.setProperty("SimplifiedChinese", value);
					index.add(objectNode, "Lemma", lemma);
				}

				// Make sure this relationship is unique
				// E.g <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, use Type
				RelationshipType relType = DynamicRelationshipType.withName(predicateName);
				boolean hit = false;
				for (Relationship rel : subjectNode.getRelationships(Direction.OUTGOING, relType)) {
					if (rel.getEndNode().equals(objectNode)) {
						hit = true;
					}
				}

				if (!hit) { // Only create relationship, if it didn't exist
					subjectNode.createRelationshipTo(objectNode, DynamicRelationshipType.withName(predicateName)).setProperty("Eng_CH",
							predicate.stringValue());
				}
				
				for (Relationship rel : subjectNode.getRelationships(Direction.INCOMING, relType)) {
					if (rel.getEndNode().equals(objectNode)) {
						hit = true;
					}
				}

				if (!hit) { // Only create relationship, if it didn't exist
					objectNode.createRelationshipTo(subjectNode, DynamicRelationshipType.withName(predicateName)).setProperty("CH_Eng",
							predicate.stringValue());
				}
				
			} 

			totalNodes++;

			long nodeDelta = totalNodes - sinceLastCommit;
			long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			//if (nodeDelta >= 115929 || timeDelta >= 5) { // Commit every 150k operations or every 30 seconds
			//if (totalNodes==51705 || nodeDelta >= 100000){	
			if (totalNodes==51705 || nodeDelta >= 100000){
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
