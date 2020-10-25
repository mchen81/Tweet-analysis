package util;

import Constants.Constants;

import java.util.Arrays;
import java.util.HashSet;

public class Utilities {
    /**
     *
     * @param day
     * @return the number of week, only 1 - 4
     */
    public static int getWeek(int day){
        int week = ((day - 1) / 7) + 1;

        return week >= 5 ? 0: week;
    }

    /**
     *
     * @return a hash set that comprised of all users in Constants.lucky120
     */
    public static HashSet<String> getLucky120Set(){
        return new HashSet<>(Arrays.asList(Constants.lucky120.split("\n")));
    }
}
