package util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NormalActiveUserCheck {
    public final static Set<String> normalActiveUserSet = new HashSet<>();

    static {
        String nAUFile = "NormalActiveUsers/step1_candidates.txt";
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(nAUFile));
            line = reader.readLine();
            while (line != null) {
                String[] sArray = line.split("\t");
                if(sArray.length == 2) normalActiveUserSet.add(sArray[0]);
                line = reader.readLine();
            }
            reader.close();
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("cannot find sentiment files");
        } catch (IOException e) {
            throw new IllegalArgumentException("IOException happened: " + e.getMessage());
        }
    }

    public static boolean isNormalActiveUser(String userName){
        return normalActiveUserSet.contains(userName);
    }
}
