package reducer;


import java.util.*;
import java.util.Date;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import writable.FraudWritable;

//Key value pair input for reducer
//customerID      name|date product received|returned product|returned date
//GGYZ333519YS   [{Allison 06-01-2017,no,null} {Allison 26-01-2017,yes,16-02-2017} {Allison 13-01-2017,yes,15-01-2017}  {Allison 07-01-2017,no,null} 
public class FraudReducer extends Reducer<Text, FraudWritable, Text, IntWritable>
{
	
	ArrayList<String> customers = new ArrayList<String>();

	/*
	 * 1st task is assign one point for each fraudulent transaction (return date is less than 10 days of received)
	 * 2nd task is to calculate overall return rate percentage.
	 * */
	@Override
	protected void reduce(Text key, Iterable<FraudWritable> values, Context c)	throws IOException, java.lang.InterruptedException
	{
		int fraudPoints = 0;
		int returnsCount = 0;
		int total_number_of_orders = 0;

		// initialiszing writable class to null
		FraudWritable transaction = null;        
		
		//creating iterator to loop through key value pairs it points to values
		Iterator<FraudWritable> valuesIter = values.iterator();

		//GGYZ333519YS   [{Allison 06-01-2017,no,null} {Allison 26-01-2017,yes,16-02-2017} {Allison 13-01-2017,yes,15-01-2017}  {Allison 07-01-2017,no,null} 
		//We will iterate through all values(transactions) from customerId Key.
		while (valuesIter.hasNext())
		{
			//Each value is an order transaction and needs to be increased.
			total_number_of_orders++;                      

			//storing information from customer gathered from iterator into data
			//name | date product received | returned product | returned date
			//transaction =  Allison 26-01-2017,yes,16-02-2017
			transaction = valuesIter.next();     

			//Check if order was returned or not.
			//We need to check if the transaction get Returned is not null
			if (transaction.getReturned())
			{
				//update return counter
				returnsCount++;
				
				try
				{
					/*
					 * Check if return date - received date is less than 10 days
					*/
					
					//Convert date into this format
					SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
					
					//Convert dates to date datatype
					Date receiveDate = sdf.parse(transaction.getReceiveDate());
					Date returnDate = sdf.parse(transaction.getReturnDate());

					//Date difference is stored as millliseconds
					long diffInMillies = Math.abs(returnDate.getTime() - receiveDate.getTime());
					//Convert from milliseconds to days
					long diffDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);     

					/* 1 fraud point to a customer whose (refund_date - receiving_date) > 10 days */
					if (diffDays > 10)
						fraudPoints++;            // fraudPoints  12
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}//catch
			}//if
		}//while
		
		/* 10 fraud points to the customer whose return rate is more than 50% */
		double returnRate = (returnsCount/(total_number_of_orders*1.0))*100;
		if (returnRate >= 50)
			fraudPoints += 10;

		
		//Output key value pair of customers is:
		//  [{BHEE999914ED,Ana,12} {CCWO777171WT,Arthur,12} {GGYZ333519YS,Allison,12}  {BPLA457837LB,Alex,0}.......]
		customers.add(key.toString() + "," + transaction.getCustomerName() + "," + fraudPoints);
	}
	

	//Doing sorting in cleanup method
	@Override
	protected void cleanup(Context c)throws IOException, java.lang.InterruptedException
	{
		/* sort customers based on fraudpoints */
		Collections.sort(customers, new Comparator<String>()
			{
				public int compare(String s1, String s2)
				{
					int fp1 = Integer.parseInt(s1.split(",")[2]);
					int fp2 = Integer.parseInt(s2.split(",")[2]);

					/*For desscending order*/
					return -(fp1-fp2);     
				}
			}
		);
		
		//Output key value pair from sorted list
		for (String f: customers)
		{
			String[] words = f.split(",");
                            // custID        // custname                                 // fraud points
			c.write(new Text(words[0] + "," + words[1]), new IntWritable(Integer.parseInt(words[2])));
		} 
	}
}

