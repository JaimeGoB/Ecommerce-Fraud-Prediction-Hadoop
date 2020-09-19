package writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


/*
 * The custom Writable class will be only used for outputting values
 * and not Keys, we will not compare because of this.
 * Thus, we only need a writable and not compareable.
 * 
 * The transaction information of a customers order is:
 * CustomerID, Name, DatePlacedORder, Shipping Date, Shipping Carrier, Product Received, Returned Product, Return Date, Reason for return
 * 
 * */
public class FraudWritable implements Writable
{

	
    private String customerName;
    private String receiveDate;
    private boolean returned;
    private String returnDate;
    
    //Default constructor
    //Will initialize the class to empty strings if parameters are not passed on constructor
    public FraudWritable()
    {
	set("", "", "no", "");
    }
    
    /* getters and setters for custom values */
    
    //Set value for all variables
    public void set(String customerName, String receiveDate, String returned, String returnDate) 
    {
    	this.customerName = customerName;
    	this.receiveDate = receiveDate;
    	
    	if (returned.equalsIgnoreCase("yes"))
    		this.returned = true;
    	else
    		this.returned = false;
    	
    	this.returnDate = returnDate;
    }

    public String getCustomerName()
    {
    	return this.customerName;
    }
    public String getReceiveDate()
    {
    	return this.receiveDate;
    }
    public boolean getReturned()
    {
    	return this.returned;
    }
    public String getReturnDate()
    {
    	return this.returnDate;
    }
        
    // Mandatory WRITE AND READFIELDS functions for Hadoop to know how to write values for this class
   
    //Write - function that converts data into byte streams
    @Override
    public void write(DataOutput out) throws IOException 
    {
    	WritableUtils.writeString(out, this.customerName);
    	WritableUtils.writeString(out, this.receiveDate);
    	out.writeBoolean(this.returned);
    	WritableUtils.writeString(out, this.returnDate);
    }
    // Mandatory functions for Hadoop to know how to read values for this class
   
    //readFields - function that re-constructs binary data into normal data
    @Override
    public void readFields(DataInput in) throws IOException
    {
    	this.customerName = WritableUtils.readString(in);
    	this.receiveDate = WritableUtils.readString(in);
    	this.returned = in.readBoolean();
    	this.returnDate = WritableUtils.readString(in);
    }
}
