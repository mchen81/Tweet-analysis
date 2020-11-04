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

    public static String getTweetUser(String tweeterLink) {
        if (tweeterLink == null || tweeterLink.length() < 20) {
            return null;
        }
        return tweeterLink.substring(19);
    }

    /**
     *
     * @return a random integer in a particular range
     */
    public int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }
}
