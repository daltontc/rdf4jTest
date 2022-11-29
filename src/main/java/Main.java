import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.endpoint.Endpoint;
import org.eclipse.rdf4j.federated.endpoint.EndpointFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
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
import java.util.ArrayList;
import java.util.List;
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
        repo.shutDown();

        List<Endpoint> endpoints = new ArrayList<>();
        endpoints.add(EndpointFactory.loadNativeEndpoint(store.getDataDir()));
        Repository fedXRepo = FedXFactory.createFederation(endpoints);

        try (RepositoryConnection conn = fedXRepo.getConnection()) {
            TupleQuery query = conn.prepareTupleQuery("select * where {?s ?p ?o} limit 10");
            SimpleDataset dataset = new SimpleDataset();
            dataset.addDefaultGraph(vf.createIRI("http://mobi.com/dataset/d62947b8-b957-49fa-b46a-97c26cf9d7d7_system_dng"));
            query.setDataset(dataset);

            System.out.println("Dataset: " + query.getDataset());

            // Outputs values not from the dataset
            TupleQueryResult result = query.evaluate();
            for (BindingSet solution : result) {
                System.out.println("?s = " + solution.getValue("s") + "\t" + "?p = " + solution.getValue("p") + "\t" + "?o = " + solution.getValue("o"));
            }
        }
    }
}
