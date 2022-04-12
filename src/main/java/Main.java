import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Main {
    static ValueFactory vf = SimpleValueFactory.getInstance();

    public static void main(String[] args) throws IOException {
        NativeStore nativeStore = new NativeStore(new File("target/datadir/" + UUID.randomUUID()));
        Repository repo = new SailRepository(nativeStore);

        InputStream stream = Main.class.getResourceAsStream("/data.trig");
        try (RepositoryConnection conn = repo.getConnection()) {
            conn.add(stream, RDFFormat.TRIG);
        }

        IRI recordId = vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309");
//        Model commitModel = Rio.parse(Main.class.getResourceAsStream("/commit.ttl"), RDFFormat.TURTLE);

        try (RepositoryConnection conn = repo.getConnection()) {

            // private OrmFactory<? extends VersionedRDFRecord> getFactory(Resource recordId, RepositoryConnection conn)
//            List<Resource> types = QueryResults.asList(conn.getStatements(recordId, RDF.TYPE, null))
//                    .stream()
//                    .map(Main::objectResource)
//                    .filter(Optional::isPresent)
//                    .map(Optional::get)
//                    .collect(Collectors.toList());

            // public <T extends Record> T getRecord(Resource catalogId, Resource recordId,
            // public void validateRecord(Resource catalogId, Resource recordId, IRI recordType, RepositoryConnection conn)
            // public void validateResource(Resource resource, IRI classId, RepositoryConnection conn)
//            contains(conn, vf.createIRI("http://mobi.com/catalog-local"), RDF.TYPE, vf.createIRI("http://mobi.com/ontologies/catalog#Catalog"), vf.createIRI("http://mobi.com/catalog-local"));
            // public <T extends Record> T getRecord(Resource catalogId, Resource recordId,
            // public void validateRecord(Resource catalogId, Resource recordId, IRI recordType, RepositoryConnection conn)
            // public void validateResource(Resource resource, IRI classId, RepositoryConnection conn)
//            contains(conn, vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"), RDF.TYPE, vf.createIRI("http://mobi.com/ontologies/ontology-editor#OntologyRecord"), vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"));
            // public <T extends Record> T getRecord(Resource catalogId, Resource recordId,
            // public void validateRecord(Resource catalogId, Resource recordId, IRI recordType, RepositoryConnection conn)
//            contains(conn, vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"), vf.createIRI("http://mobi.com/ontologies/catalog#catalog"), vf.createIRI("http://mobi.com/catalog-local"));
            // public <T extends Record> T getRecord(Resource catalogId, Resource recordId,
            // public <T extends Thing> T getObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            // public <T extends Thing> Optional<T> optObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            RepositoryResult<Statement> stmt;
//            stmt = conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"));
//            stmt.close();

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            conn.begin();

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public Branch getTargetBranch(T record, Resource branchId, RepositoryConnection conn)
            // public <T extends Branch> T getBranch(VersionedRDFRecord record, Resource branchId, OrmFactory<T> factory,
            // public <T extends Thing> T getObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            // public <T extends Thing> Optional<T> optObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
//            stmt = conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/branches#da70d831-1f1e-4d80-a0eb-f65f6da140a7"));
//            stmt.close();

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public Commit getBranchHeadCommit(Branch branch, RepositoryConnection conn)
            // public <T extends Thing> T getObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            // public <T extends Thing> Optional<T> optObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
//            stmt = conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/commits#eb8d41168890ad77d050752c79d1b68ab1fc9f56"));
//            stmt.close();

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public InProgressCommit getInProgressCommit(Resource recordId, User user, RepositoryConnection conn)
            // public InProgressCommit getInProgressCommit(Resource recordId, Resource userId, RepositoryConnection conn)
            // public Optional<Resource> getInProgressCommitIRI(Resource recordId, Resource userId, RepositoryConnection conn)
//            TupleQuery query = conn.prepareTupleQuery("PREFIX mcat: <http://mobi.com/ontologies/catalog#>\n" +
//                    "PREFIX prov: <http://www.w3.org/ns/prov#>\n" +
//                    "\n" +
//                    "SELECT ?commit\n" +
//                    "WHERE {\n" +
//                    "    ?commit a mcat:InProgressCommit ;\n" +
//                    "        mcat:onVersionedRDFRecord ?record ;\n" +
//                    "        prov:wasAssociatedWith ?user .\n" +
//                    "}");
//            query.setBinding("user", vf.createIRI("http://mobi.com/users/d033e22ae348aeb5660fc2140aec35850c4da997"));
//            query.setBinding("record", recordId);
//            TupleQueryResult queryResult = query.evaluate();
//            if (queryResult.hasNext()) {
//                BindingSet bindingSet = queryResult.next();
//                queryResult.close();
//            } else {
//                Optional.empty();
//            }

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public InProgressCommit getInProgressCommit(Resource recordId, User user, RepositoryConnection conn)
            // public InProgressCommit getInProgressCommit(Resource recordId, Resource userId, RepositoryConnection conn)
            // public <T extends Thing> T getObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            // public <T extends Thing> Optional<T> optObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
//            stmt = conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/in-progress-commits#13b4f8d6-8cc0-4fad-ba27-002802f1201c"));
//            stmt.close();

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public void addCommit(Branch branch, Commit commit, RepositoryConnection conn)
            // private Optional<Resource> getRecordIriIfMaster(Branch branch, RepositoryConnection conn)
//            TupleQuery query2 = conn.prepareTupleQuery("PREFIX mcat: <http://mobi.com/ontologies/catalog#>\n" +
//                    "PREFIX ontedit: <http://mobi.com/ontologies/ontology-editor#>\n" +
//                    "\n" +
//                    "SELECT ?record\n" +
//                    "WHERE {\n" +
//                    "\t?branch a mcat:Branch .\n" +
//                    "\n" +
//                    "    ?record a ontedit:OntologyRecord ;\n" +
//                    "        mcat:branch ?branch ;\n" +
//                    "        mcat:masterBranch ?branch .\n" +
//                    "}");
//            query2.setBinding("branch", vf.createIRI("https://mobi.com/branches#da70d831-1f1e-4d80-a0eb-f65f6da140a7"));
//            TupleQueryResult result = query2.evaluate(); // TODO: is evaluteAndReturn
//            if (!result.hasNext()) {
//                Optional.empty();
//                return;
//            }
//            result.next();
////            Optional<Resource> recordIriOpt = Optional.of(Bindings.requiredResource(result.next(), RECORD_BINDING));
//            result.close();

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public void addCommit(Branch branch, Commit commit, RepositoryConnection conn)
            // private void updateOntologyIRI(com.mobi.rdf.api.Resource recordId, Commit commit, RepositoryConnection conn)
            // public Stream<Statement> getAdditions(Commit commit, RepositoryConnection conn)
            // private Stream<Statement> getAdditions(Revision revision, RepositoryConnection conn)
//            List<Stream<Statement>> streams = new ArrayList<>();
//            RepositoryResult<Statement> statements = conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/additions#13b4f8d6-8cc0-4fad-ba27-002802f1201c"));
//            GraphRevisionIterator iterator = new GraphRevisionIterator(statements.iterator(), null);
//            streams.add(StreamSupport.stream(iterator.spliterator(), false));
//            Stream<Statement> strStatements = streams.stream()
//                    .reduce(Stream::concat)
//                    .orElseGet(Stream::empty);
//            strStatements.filter(statement ->
//                            statement.getPredicate().equals(RDF.TYPE) &&
//                                    statement.getObject().equals(vf.createIRI(OWL.ONTOLOGY.stringValue())))
//                    .findFirst()
//                    .flatMap(statement -> Optional.of(statement.getSubject()));

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public void addCommit(Branch branch, Commit commit, RepositoryConnection conn)
            // private void updateOntologyIRI(com.mobi.rdf.api.Resource recordId, Commit commit, RepositoryConnection conn)
            // public <T extends Thing> T getObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            // public <T extends Thing> Optional<T> optObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
//            stmt = conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"));
//            stmt.close();

//            try (RepositoryConnection conn = configProvider.getRepository().getConnection()) {
//                TupleQuery query = conn.prepareTupleQuery(FIND_ONTOLOGY);
//                query.setBinding(ONTOLOGY_IRI, ontologyIRI);
//                query.setBinding(CATALOG, configProvider.getLocalCatalogIRI());
//                TupleQueryResult result = query.evaluate();
//                boolean exists = result.hasNext();
//                result.close();
//                return exists;
//            }

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public void addCommit(Branch branch, Commit commit, RepositoryConnection conn)
            // private void updateOntologyIRI(com.mobi.rdf.api.Resource recordId, Commit commit, RepositoryConnection conn)
            // public <T extends Thing> void updateObject(T object, RepositoryConnection conn)
            // public <T extends Thing> void removeObject(T object, RepositoryConnection conn)
            // public void remove(Resource resourceId, RepositoryConnection conn)
            conn.remove((Resource) null, null, null, vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"));

            // public Resource commit(Resource catalogId, Resource recordId, Resource branchId, User user, String message)
            // private Resource commitToBranch(VersionedRDFRecord record, VersioningService<VersionedRDFRecord> service,
            // public void addCommit(Branch branch, Commit commit, RepositoryConnection conn)
            // private void updateOntologyIRI(com.mobi.rdf.api.Resource recordId, Commit commit, RepositoryConnection conn)
            // public <T extends Thing> void updateObject(T object, RepositoryConnection conn)
            // public <T extends Thing> void addObject(T object, RepositoryConnection conn)
            conn.add(Rio.parse(Main.class.getResourceAsStream("/record_def.trig"), RDFFormat.TRIG), vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"));
            stmt = conn.getStatements(null, vf.createIRI(RDF.TYPE.stringValue()), vf.createIRI("http://mobi.com/ontologies/catalog#VersionedRDFRecord"));
//            stmt.forEach(statement -> {
//                RepositoryResult<Statement> stmt2 = conn.getStatements(statement.getSubject(), null, null);
//                Model model = QueryResults.asModel(stmt2);
//                stmt2.close();
//            });
            RepositoryResult<Statement> stmt3 = conn.getStatements(vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309"), null, null);
            Model model = QueryResults.asModel(stmt3);
//            stmt3.close();
            stmt.close();




            // public <T extends Thing> T getObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            // public <T extends Thing> Optional<T> optObject(Resource id, OrmFactory<T> factory, RepositoryConnection conn)
            Model resultFinal = QueryResults.asModel(conn.getStatements(null, null, null, vf.createIRI("https://mobi.com/records#8b2ed314-e46b-46f0-aa68-f2e0fa2ea309")));
            System.out.println(resultFinal.size());

        }
    }

    public static Optional<Resource> objectResource(Statement statement) {
        Value object = statement.getObject();
        if (object instanceof Resource) {
            return Optional.of((Resource) object);
        } else {
            return Optional.empty();
        }
    }

    public static boolean contains(RepositoryConnection conn, Resource subject, IRI predicate, Value object, Resource... contexts) {
        RepositoryResult<Statement> results = conn.getStatements(subject, predicate, object, contexts);
        boolean contains = results.hasNext();
        results.close();
        return contains;
    }

    private static class GraphRevisionIterator implements Iterator<Statement>, Iterable<Statement> {
        private final Iterator<Statement> delegate;
        private final Resource graph;

        GraphRevisionIterator(Iterator<Statement> delegate, Resource graph) {
            this.delegate = delegate;
            this.graph = graph;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Statement next() {
            Statement statement = delegate.next();
            return vf.createStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), graph);
        }

        @Override
        public Iterator<Statement> iterator() {
            return this;
        }
    }
}
