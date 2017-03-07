import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class ReduceImpl implements IReducer{

	public String reduce(String s) {
	
		String s_output="";

		BufferedReader bufReader = new BufferedReader(new StringReader(s));
		String line=null;
		try {
			while( (line=bufReader.readLine()) != null )
			{
				String namepass[] = line.split(":"); 
				s_output += namepass[1];
				s_output += "\n";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return s_output;
	}
}
