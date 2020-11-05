package Test;

import util.NormalActiveUserCheck;
import util.WordSentimentUtil;

public class TestMain {
    public static void main(String[] args) {
        new NormalActiveUserCheck();
        //new WordSentimentUtil();

      //  System.out.println(WordSentimentUtil.getWordScore("fuck"));
      //  System.out.println(WordSentimentUtil.getWordScore("love"));

        System.out.println(NormalActiveUserCheck.isNormalActiveUser("ddddd"));
        System.out.println(NormalActiveUserCheck.isNormalActiveUser("abledragon"));
    }
}
