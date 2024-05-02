package dai_collateral;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ContractAddressProcessor {
	
	final static String CONTRACT_ADDRESS_LIST = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\dai_addresses.csv";
	
	final static String OUTPUT_FOLDER = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\programmatuur\\java\\blockchain_ntu\\dai_collateral\\output\\addresses\\";
	
	public void readAddressesWithKeyword(String keyword, String outputFileName) {
		List<String> strings = new ArrayList<String>();
		
		BufferedReader reader;
		
		try {
			reader = new BufferedReader(new FileReader(CONTRACT_ADDRESS_LIST));
			String line = reader.readLine(); // first line denotes the columns names

			while ((line = reader.readLine()) != null) {
				String[] lineData = line.split(",");
				if(lineData.length > 1) {
					String name = lineData[0];
					String address = lineData[1].toLowerCase();
					
					if(name.contains(keyword)) {
						strings.add(name+","+address);
					}
				}
			}
			
			PrintWriter writer = new PrintWriter(OUTPUT_FOLDER+outputFileName);
            
    		for(String s : strings) {
    			writer.println(s);
    		}
            
            writer.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		// by type
		readAddressesWithKeyword("JOIN", "join_addresses.csv");
		readAddressesWithKeyword("CLIP", "clip_addresses.csv");
		
		// by token
		readAddressesWithKeyword("ETH", "eth_addresses.csv");
	}
	
	public static void main(String[] a) {
		new ContractAddressProcessor().run();
	}
}
