import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class Main {
    static ValueFactory vf = SimpleValueFactory.getInstance();

    public static void main(String[] args) throws IOException {
        IRI context = vf.createIRI("https://test.com/context");
        Path path = Paths.get("target/test");
        if (Files.exists(path)) {
            System.out.println("Deleting old repo");
            FileUtils.deleteDirectory(path.toFile());
        }

        NativeStore store = new NativeStore();
        store.setTripleIndexes("spoc,posc,cspo,opsc");
        store.setDataDir(path.toFile());
        Repository repo = new SailRepository(store);
        repo.init();
        try (RepositoryConnection conn = repo.getConnection()) {
            System.out.println("Loading in data");
            InputStream is = Main.class.getResourceAsStream("/test.trig");
            Model model = Rio.parse(is, "", RDFFormat.TRIG);
            conn.add(model);
            System.out.println("Loading in data COMPLETE");
        }
        try (RepositoryConnection conn = repo.getConnection()) {
            TupleQuery query = conn.prepareTupleQuery("prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    "prefix owl: <http://www.w3.org/2002/07/owl#>\n" +
                    "\n" +
                    "select ?parent ?child\n" +
                    "where {\n" +
                    "    values ?type {owl:Class rdfs:Class}\n" +
                    "    ?parent rdf:type ?type .\n" +
                    "    optional {\n" +
                    "        ?child rdfs:subClassOf ?parent ;\n" +
                    "               rdf:type ?type .\n" +
                    "    }\n" +
                    "}");
            TupleQueryResult results = query.evaluate();
            System.out.println("Iterating over query results");
            StopWatch watch = new StopWatch();
            watch.start();
            int parentCount = 0;
            int childCount = 0;
            while (results.hasNext()) {
                BindingSet result = results.next();
                Value key = Optional.ofNullable(result.getBinding("parent")).orElseThrow(
                        () -> new RuntimeException("Parent binding must be present for hierarchy")).getValue();
                Binding value = Optional.ofNullable(result.getBinding("child"))
                        .orElse(Optional.ofNullable(result.getBinding("individual")).orElse(null));
                if (value != null) {
                    parentCount++;
                    System.out.println(key.stringValue() + "       " + value.getValue().stringValue());
                } else {
                    parentCount++;
                    childCount++;
                    System.out.println(key.stringValue());
                }
            }
            System.out.println("Iterating over query results COMPLETE");
            System.out.println("Total query time: " + watch.getTime());
            System.out.println("Parent Count: " + parentCount);
            System.out.println("Child Count: " + childCount);
        }
    }
}
