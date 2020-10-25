package localTasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Use Microsoft Azure Cognitive Services to do sentiment analyze
 * You need hardcode the keys(user or hashtag) at main method, prepare the corresponding datasets and hardcode the path
 */
//TODO: The reason why I only use top5 users and top5 hashtags of week2 and keys, and 50 data per key is because Microsoft only gave me $100 credit
public class BA3 {
    /**
     * Modified these path as need
     * metadata: all tweets belong to the keys(user or hashtag) we want to analyze
     */
    static String userDataPath = "/Users/jcliu/Projects/usfGit/cs677/Project2/P2-chenchenpi/results/BA3/Step101";
    static String HashTagDataPath = "/Users/jcliu/Projects/usfGit/cs677/Project2/P2-chenchenpi/results/BA3/Step102";
    static String userOutputPath = "";
    static String tagOutputPath = "";

    /**
     * Arguments: $API_KEY
     * @param args
     */
    public static void main(String[] args) {
        //1: user sentiment analyze
        ArrayList<String> list = new ArrayList<>();
        //top5 user(actually chosen user)
//        list.add("delicious50");
//        list.add("thinkingstiff");
//        list.add("dominiquerdr");
//        list.add("mariolavandeira");
//        list.add("thegamingscoop");

        //top5 hashtag of week2
        list.add("iranelection");
        list.add("jobs");
        list.add("followfriday");
        list.add("spymaster");
        list.add("iremember");
        try{
            startAnalyze(list, args[0], HashTagDataPath);
        }catch (IOException e) {
            System.out.println(e);
        }
    }

    /**
     * Give it keyList, APIKey, path of metaData, it will do sentiment analyze and print
     *
     */
    public static void startAnalyze(ArrayList<String> keyLit, String APIKey, String dataPath) throws IOException {
        HashMap<String, List<String>> map = new HashMap<>();

        for(String key : keyLit) map.put(key, new ArrayList<>());

        int counter = 0;

        //Step1: prepare lists
        BufferedReader br = new BufferedReader(new FileReader(dataPath));

        String line = br.readLine();
        while(line != null && counter < 5){
            String[] sA = line.split("X=====X");
            if(sA.length < 2) {
                line  = br.readLine();
                continue;
            }

            String user = sA[0];
            if(!map.containsKey(user)
                    || map.get(user).size() >= 50) {
                line = br.readLine();
                continue;
            }

            List<String> list = map.get(user);
            String tweet = sA[1].replaceAll("[^a-zA-Z0-9@#]", "");
            list.add(tweet);
            if(list.size() >= 50) counter++;

            line  = br.readLine();
        }

        //Step2: start sentiment analyze!
        for(String key : map.keySet()){
            System.out.println(key + ": " + sentimentAnalysis(map.get(key), APIKey));
        }
        System.out.println();
    }


    /**
     * input: a list contains a batch of strings that we want to analyze
     * @return total weighted positive/negative score
     */
    private static int sentimentAnalysis(List<String> stringList, String APIKey){
        int score = 0;

        ArrayList<String> list = new ArrayList<>();
        for(String s : stringList){
            list.add(s);
            if(list.size() == 10){
                score += apiCall(list, APIKey);
                list = new ArrayList<>();
            }
        }

        return score;
    }

    private static int apiCall(List<String> stringList, String APIKey){
        HttpClient httpclient = HttpClients.createDefault();
        JSONObject jsonObject = getRequestBody(stringList);

        int score = 0;

        try
        {
            URIBuilder builder = new URIBuilder(
                    "https://eastus.api.cognitive.microsoft.com/text/analytics/v3.1-preview.1/sentiment");

            URI uri = builder.build();
            HttpPost request = new HttpPost(uri);
            request.setHeader("Content-Type", "application/json");
            request.setHeader("Ocp-Apim-Subscription-Key", APIKey);

            // Request body
            StringEntity reqEntity = new StringEntity(jsonObject.toString());
            request.setEntity(reqEntity);

            HttpResponse response = httpclient.execute(request);
            HttpEntity entity = response.getEntity();

            if (entity != null)
            {
                JSONObject obj = new JSONObject(EntityUtils.toString(entity));
                //System.out.println(obj);

                JSONArray arr = obj.getJSONArray("documents");

                for (int i = 0; i < arr.length(); i++) {
                    String sentiments = arr.getJSONObject(i).getString("sentiment");
                    if(sentiments.equals("negative")) score--;
                    else if(sentiments.equals("positive")) score++;
                }
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }

        return score;
    }

    private static JSONObject getRequestBody(List<String> stringList){
        JSONObject jsonObject = new JSONObject();

        JSONArray array = new JSONArray();

        int id = 1;
        for (String sentence : stringList){
            JSONObject sObj = new JSONObject();
            sObj.put("language", "en");
            sObj.put("id", Integer.toString(id));
            sObj.put("text", sentence);

            array.put(sObj);

            id++;
        }

        jsonObject.put("documents", array);

        return jsonObject;
    }
}

