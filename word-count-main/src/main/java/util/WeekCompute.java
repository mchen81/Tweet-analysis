package util;

public class WeekCompute {
    /**
     *
     * @param day
     * @return the number of week, only 1 - 4
     */
    public static int getWeek(int day){
        int week = ((day - 1) / 7) + 1;

        return week >= 5 ? 0: week;
    }
}
