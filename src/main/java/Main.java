import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws IOException {
        ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
        IRI subcont = VALUE_FACTORY.createIRI("urn:test");
        IRI someClass = VALUE_FACTORY.createIRI("urn:someClass");

        NativeStore nativeStore = new NativeStore(new File("target/datadir/" + UUID.randomUUID()));
        Repository repo = new SailRepository(nativeStore);
        repo.initialize();

        try (RepositoryConnection conn = repo.getConnection()) {
            Model startModel = new LinkedHashModel();
            startModel.add(subcont, RDF.TYPE, someClass);
            startModel.add(subcont, RDF.TYPE, VALUE_FACTORY.createIRI("urn:someParentClass"));

            conn.add(startModel, subcont);
            
            System.out.println("startModel: " + startModel.size());
            printModel(startModel);

            conn.getStatements(subcont, RDF.TYPE, someClass);
            
            conn.clear(subcont);
            conn.add(startModel, subcont);

            conn.getStatements(subcont, RDF.TYPE, someClass).hasNext();

            Model modelFromContext = QueryResults.asModel(conn.getStatements(null, null, null, subcont));
            System.out.println("modelFromContext: " + modelFromContext.size());
            printModel(modelFromContext);

            Model modelFromSubject = QueryResults.asModel(conn.getStatements(subcont, null, null));
            System.out.println("modelFromSubject: " + modelFromSubject.size());
            printModel(modelFromSubject);
        }
    }

    private static void printModel(Model model) {
        model.forEach(statement -> {
            System.out.println("\t\t" + statement.toString());
        });
    }
}
