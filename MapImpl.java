import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapImpl implements IMapper{

	public String map(String s) {	
		int cnt=1;
		String s_output="";
		Pattern p = Pattern.compile("[a-z].*map");

			BufferedReader bufReader = new BufferedReader(new StringReader(s));
			String line=null;
			try {
				while( (line=bufReader.readLine()) != null )
				{
					Matcher m = p.matcher(line);
					if (m.find()){
						s_output += Integer.toString(cnt);
						s_output += " :";
						s_output += line;
						s_output += "\n";
						cnt++;
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return s_output;		
	}	
}
