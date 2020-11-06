# P2-JerryAndJiachen

Draft report: https://docs.google.com/document/d/1YIgvystgEzhgasAJKksMcnQ6aWyJd_Xclvkgk1k8lhg/edit?usp=sharing

## Advanced Analysis:

  Step0: count the number of tweets per user and save to `/outAA01`

  Step1: find out "normal active users": 
  
    yarn jar ./wordcount-1.0.jar AdvancedAnalysis.Step1.NormalActiveUser /outAA01 /outAA02
    
  Step2: compute total mood score of each hour:
  
     yarn jar ./wordcount-1.0.jar AdvancedAnalysis.AnalyzeByDictionary.ComputeScore.AASentimentAnalysis /outAA02/part-r-00000 /outAA03
     
  Step3: count the number of tweets from "normal active users" per hour:
     
     yarn jar ./wordcount-1.0.jar AdvancedAnalysis.AnalyzeByDictionary.CountNumber.AASentimentAnalysis /outAA02/part-r-00000 /outAA03
     
  Step4: Now, we have total score and number of tweets for each hour. Do `FinalScore = TotalScore / NumberOfTweets` 24 times on our calculator to get the final result.

## Final Project Dataset Prepeocess:

  After uploaded our dataset to HDFS, to the following job:
  
    yarn jar ./wordcount-1.0.jar FinalProject.JsonToText /All_Amazon_Review_5.json /final01
