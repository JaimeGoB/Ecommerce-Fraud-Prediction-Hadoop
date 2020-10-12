package mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import writable.FraudWritable;

//This is the input values mapper will receive:
// CustomerID, Name, Date Order Placed, Shipping Date, Shipping Carrier, Date Product Received, Returned Product, Return Date, Reason for return
//GGYZ333519YS,Allison,01-01-2017,03-01-2017,Fedx,06-01-2017,no,null,null
public class FraudMapper extends Mapper<LongWritable, Text, Text, FraudWritable>
{

	//This will be use to store customer id (Key for mapper)
	//ex:
	//GGYZ333519YS 
	private Text custId = new Text();    

	// will create object of writable class and initialize it to empty strings (value for mapper)
	private FraudWritable data = new FraudWritable();    

	@Override
	protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
	{

		//Reading entire line in file
		//ex:
		//GGYZ333519YS,Allison,01-01-2017,03-01-2017,Fedx,06-01-2017,no,null,null
		String line = value.toString();    
		
		//Split line by delimiter to get customer information
		// [{GGYZ333519YS} {Allison} {01-01-2017} {03-01-2017} {Fedx} {06-01-2017} {no} {null} {null}]
		//        0           1            2           3          4        5        6     7       8
		String[] customer_information = line.split(",");  

		
		//settting up custdid = GGYZ333519YS
		custId.set(customer_information[0]);             

		//setting up variables in writable:
		//                   name          date product received       returned product          returned date
		//                  {Allison}           {06-01-2017}                {no}                   {null}
		data.set(customer_information[1], customer_information[5], customer_information[6], customer_information[7]);

		// KEY: 	AGGYZ333519YS
		// VALUE:	{Allison}           {06-01-2017}                {no}                   {null}	
		c.write(custId, data);
	}
}

