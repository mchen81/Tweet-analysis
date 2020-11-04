package util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class WordSentimentUtil {

    public final static Set<String> positiveWordsSet = new HashSet<>();
    public final static Set<String> negativeWordsSet = new HashSet<>();

    static {
        String positiveTextFile = "SentimentWords/Positive.txt";
        String negativeTextFile = "SentimentWords/Negative.txt";
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(positiveTextFile));
            line = reader.readLine();
            while (line != null) {
                positiveWordsSet.add(line);
                line = reader.readLine();
            }
            reader.close();
            reader = new BufferedReader(new FileReader(negativeTextFile));
            line = reader.readLine();
            while (line != null) {
                negativeWordsSet.add(line);
                line = reader.readLine();
            }
            reader.close();

        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("cannot find sentiment files");
        } catch (IOException e) {
            throw new IllegalArgumentException("IOException happened: " + e.getMessage());
        }


    }

    public static int getWordScore(String word) {
        word = word.toLowerCase();
        if (positiveWordsSet.contains(word)) {
            return 1;
        } else if (negativeWordsSet.contains(word)) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int getSentenceScore(String sentence) {
        StringTokenizer stringTokenizer = new StringTokenizer(sentence.toLowerCase());
        int score = 0;
        while (stringTokenizer.hasMoreTokens()) {
            score += getWordScore(stringTokenizer.nextToken());
        }
        return score;
    }


}
