import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class Main {
    static ValueFactory vf = SimpleValueFactory.getInstance();
    static IRI recordId = vf.createIRI("https://mobi.com/records#someRecord");

    public static void main(String[] args) throws IOException {
        File repoDir = new File("target/datadir/" + UUID.randomUUID());
        NativeStore nativeStore = new NativeStore();
        MemoryStore memoryStore = new MemoryStore();
        Repository repo = new SailRepository(memoryStore);

        InputStream stream = Main.class.getResourceAsStream("/record_def_original.trig");
        try (RepositoryConnection conn = repo.getConnection()) {
            conn.add(stream, RDFFormat.TRIG);
            RepositoryResult<Statement> stmts = conn.getStatements(recordId, null, null);
            Model model = QueryResults.asModel(stmts);
            stmts.close();
            System.out.println(model.size());
        }

        try (RepositoryConnection conn = repo.getConnection()) {
            conn.begin(); // Occurs with any transaction level > NONE
            conn.remove((Resource) null, null, null, recordId);
            // Clear produces the same result
            // conn.clear(recordId);
            conn.add(Rio.parse(Main.class.getResourceAsStream("/record_def_change.trig"), RDFFormat.TRIG));

            // Retrieval by graph provides expected result
            // RepositoryResult<Statement> stmts = conn.getStatements(null, null, null, recordId);
            RepositoryResult<Statement> stmts = conn.getStatements(recordId, null, null);
            Model model = QueryResults.asModel(stmts);
            System.out.println(model.size());
            stmts.close();
            conn.commit(); // Same behavior if moved below last retrieval

            RepositoryResult<Statement> recordGraph = conn.getStatements(null, null, null, recordId);
            Model resultFinal = QueryResults.asModel(recordGraph);
            recordGraph.close();
            System.out.println(resultFinal.size());
        }

        repoDir.delete();
    }
}
