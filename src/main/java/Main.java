import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws IOException {
        String indexes = "spoc,posc,cosp";
        File dataDir = Paths.get("target","data1").toFile();

        Sail baseSail = new NativeStore(dataDir, indexes);
        Repository repo1 = new SailRepository(baseSail);
        repo1.init();

        try (RepositoryConnection conn = repo1.getConnection()) {
            conn.clear();
            InputStream stream = Main.class.getResourceAsStream("/A.rdf");
            conn.add(stream, "", RDFFormat.RDFXML);

            RepositoryResult<Statement> statements = conn.getStatements(null, null, null);
            Model model = QueryResults.asModel(statements);

            Path tmpFile = Files.createFile(Paths.get("target", UUID.randomUUID() + ".rdf"));
            try (OutputStream outputStream = Files.newOutputStream(tmpFile)) {
                RDFWriter writer = Rio.createWriter(RDFFormat.RDFXML, outputStream);
                Rio.write(model, writer);
            }

            Path tmpFile2 = Files.createFile(Paths.get("target", UUID.randomUUID() + ".trig"));
            try (OutputStream outputStream = Files.newOutputStream(tmpFile2)) {
                RDFWriter writer = Rio.createWriter(RDFFormat.TRIG, outputStream);
                Rio.write(model, writer);
            }

            Path tmpFile3 = Files.createFile(Paths.get("target", UUID.randomUUID() + ".ttl"));
            try (OutputStream outputStream = Files.newOutputStream(tmpFile3)) {
                RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, outputStream);
                Rio.write(model, writer);
            }
        }
    }
}
