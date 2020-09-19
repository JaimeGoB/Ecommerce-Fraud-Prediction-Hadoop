# Ecommerce-Fraud-Prediction-Hadoop

In this program I will implement a Map-Reduce Algorithm to detect fraud from an ecommerce dataset. 

First things first, loyal customers will have multiple transactions in the dataset. Some of the transaction could be fraud or real. This complex problem could be tackle different ways. 

How I would flag a possible **fraudulent** transaction will be:

+ if the return date after an item has been received is over 10 days. If this is true then I will assign 1 fraudulent point per item refunded after 10 days.  

+ I will also keep track of average transaction refund over the total number of transactions. 
If a customer returns more than 50% of transactions I will assign the customer 10 fraudulent points. 

Here is an example of Allison:

![image 1](https://github.com/JaimeGoB/Ecommerce-Fraud-Prediction-Hadoop/blob/master/Fraud-Detection/src/data/history.png)


#### Dataset 
The transaction information of a customers order is:
+ CustomerID, 
+ Name, 
+ DatePlacedORder, 
+ Shipping Date, 
+ Shipping Carrier, 
+ Product Received, 
+ Returned Product, 
+ Return Date, 
+ Reason for return

#### Fraud Analysis from dataset

![image 1](https://github.com/JaimeGoB/Ecommerce-Fraud-Prediction-Hadoop/blob/master/Fraud-Detection/src/data/fraud-points.png)

 
