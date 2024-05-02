package dai_collateral;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeProcessor {
	
	public static String INPUT_FILE_DIR = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\graphs\\portfolio_opti_raw\\";
	public static String OUTPUT_FILE_DIR = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\graphs\\portfolio_opti_proc\\";
	
	public void processFile(String fileName) {
		BufferedReader reader;
		
		try {
			PrintWriter writer = new PrintWriter(OUTPUT_FILE_DIR+fileName);
            
			reader = new BufferedReader(new FileReader(INPUT_FILE_DIR+fileName));
			String line = reader.readLine(); // first line denotes the columns names
			
			writer.println("time"+line);
			int counter = 0;

			while ((line = reader.readLine()) != null) {
				String[] lineData = line.split(",");
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
			    Date parsedDate = dateFormat.parse(lineData[0]);
			    if(counter % 2 == 1) {
			    	writer.print(parsedDate.getTime()/1000+1);
				    for(int j=1;j<lineData.length;j++) {
				    	writer.print(","+lineData[j]);
				    }
				    writer.println();
				}
			    counter++;
			}
			
			reader.close();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void runAll() {
		File folder = new File(INPUT_FILE_DIR);
		File[] listOfFiles = folder.listFiles();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				processFile(listOfFiles[i].getName());
			}
		}
	}

	public static void main(String[] a) {
		new TimeProcessor().runAll();
	}
}
