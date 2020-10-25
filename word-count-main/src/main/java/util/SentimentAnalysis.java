package util;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class SentimentAnalysis {
    /**
     * input: a list contains a batch of strings that we want to analyze
     * And also need APIKey for Azure Cognitive Services
     * @return total weighted positive/negative score
     */
    public static int sentimentAnalysis(List<String> stringList, String APIKey){
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

//            if(response.getStatusLine().getStatusCode() != 200){
//                System.out.println(response);
//            }

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
           // System.out.println(e.getMessage());
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
