import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class Main {

    public static void main(String[] args) throws IOException {
        InputStream stream = Main.class.getResourceAsStream("/test.ttl");
        StringWriter sw = new StringWriter();
        RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
        RDFWriter writer = org.eclipse.rdf4j.rio.Rio.createWriter(RDFFormat.JSONLD, sw);
        parser.setRDFHandler(writer);
        parser.parse(stream, "");
        String val = sw.toString();
        System.out.println(val);
    }
}
