package Test;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.simple.*;
import java.util.*;
import edu.stanford.nlp.pipeline.*;

public class SimpleCoreNLPDemo {
    public static void main(String[] args) {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // read some text in the text variable
        String text = "Hello, nice to meet you!";

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // run all Annotators on this text
        pipeline.annotate(document);
    }
}