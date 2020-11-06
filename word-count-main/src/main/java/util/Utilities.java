package util;

public class Utilities {
    /**
     *
     * @param day: the day of a month
     * @return the number of week, only 1 - 4
     */
    public static int getWeek(int day){
        int week = ((day - 1) / 7) + 1;

        return week >= 5 ? 0: week;
    }

    /**
     *
     * @param tweeterLink: the link of a tweet
     * @return the userID
     */
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
