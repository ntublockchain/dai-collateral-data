package dai_collateral;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class DataCombiner {
	
	int CENTS_PER_DOLLAR = 100;
	
	String DATA_DIR = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\bigquery\\analysis_data\\";
	
	String[][] DATA_FILE_PAIRS = {
		// crypto tokens
		{"join_logs\\aave_all.csv", "price_data_gemini\\aave_usd.csv", "aave", "1000000000000000", "1000"},
		{"join_logs\\bal_all.csv", "price_data_gemini\\bal_usd.csv", "bal", "1000000000000000", "1000"},
		{"join_logs\\bat_all.csv", "price_data_gemini\\bat_usd.csv", "bat", "1000000000000000", "1000"},
		{"join_logs\\comp_all.csv", "price_data_gemini\\comp_usd.csv", "comp", "1000000000000000", "1000"},
		{"join_logs\\eth_a_all.csv", "price_data_gemini\\eth_usd.csv", "eth_a", "1000000000000000", "1000"},
		{"join_logs\\eth_b_all.csv", "price_data_gemini\\eth_usd.csv", "eth_b", "1000000000000000", "1000"},
		{"join_logs\\eth_c_all.csv", "price_data_gemini\\eth_usd.csv", "eth_c", "1000000000000000", "1000"},
		{"join_logs\\gno_all.csv", "price_data_bitfinex\\gno_usd.csv", "gno", "1000000000000000", "1000"},
		{"join_logs\\knc_all.csv", "price_data_gemini\\knc_usd.csv", "knc", "1000000000000000", "1000"},
		{"join_logs\\link_all.csv", "price_data_gemini\\link_usd.csv", "link", "1000000000000000", "1000"},
		{"join_logs\\lrc_all.csv", "price_data_gemini\\lrc_usd.csv", "lrc", "1000000000000000", "1000"},
		{"join_logs\\mana_all.csv", "price_data_gemini\\mana_usd.csv", "mana", "1000000000000000", "1000"},
		{"join_logs\\matic_all.csv", "price_data_gemini\\matic_usd.csv", "matic", "1000000000000000", "1000"},
		{"join_logs\\reth_all.csv", "price_data_gemini\\eth_usd.csv", "reth", "1000000000000000", "1000"},
		{"join_logs\\uni_all.csv", "price_data_gemini\\uni_usd.csv", "uni", "1000000000000000", "1000"},
		{"join_logs\\wbtc_a_all.csv", "price_data_gemini\\btc_usd.csv", "wbtc_a", "1000", "100000"},
		{"join_logs\\wbtc_b_all.csv", "price_data_gemini\\btc_usd.csv", "wbtc_b", "1000", "100000"},
		{"join_logs\\wbtc_c_all.csv", "price_data_gemini\\btc_usd.csv", "wbtc_c", "1000", "100000"},
		{"join_logs\\wsteth_a_all.csv", "price_data_gemini\\eth_usd.csv", "wsteth_a", "1000000000000000", "1000"},
		{"join_logs\\wsteth_b_all.csv", "price_data_gemini\\eth_usd.csv", "wsteth_b", "1000000000000000", "1000"},
		{"join_logs\\yfi_all.csv", "price_data_gemini\\yfi_usd.csv", "yfi", "1000000000000000", "1000"},
		{"join_logs\\zrx_all.csv", "price_data_gemini\\zrx_usd.csv", "zrx", "1000000000000000", "1000"},
//		// stablecoins
		{"join_logs\\gusd_all.csv", "", "gusd", "1", "100"},
		{"join_logs\\usdc_a_all.csv", "", "usdc_a", "1000", "1000"},
		{"join_logs\\usdc_b_all.csv", "", "usdc_b", "1000", "1000"},
		{"join_logs\\paxusd_all.csv", "", "usdp", "1000000000000000", "1000"},
		{"join_logs\\tusd_all.csv", "", "tusd", "1000000000000000", "1000"},
		// psm
//		{"join_logs\\psm_gusd_all.csv", "", "psm_gusd", "1000", "1000"},
//		{"join_logs\\psm_pax_all.csv", "", "psm_pax", "1000", "1000"},
		{"join_logs\\psm_usdc_all.csv", "", "psm_usdc", "1000", "1000"},
		// RAW
		{"join_logs\\rwa001_all.csv", "price_data_rwa\\rwa001.csv", "rwa001", "1000000000000000", "1000"},
		{"join_logs\\rwa002_all.csv", "price_data_rwa\\rwa002.csv", "rwa002", "1000000000000000", "1000"},
		{"join_logs\\rwa003_all.csv", "price_data_rwa\\rwa003.csv", "rwa003", "1000000000000000", "1000"},
		{"join_logs\\rwa004_all.csv", "price_data_rwa\\rwa004.csv", "rwa004", "1000000000000000", "1000"},
		{"join_logs\\rwa005_all.csv", "price_data_rwa\\rwa005.csv", "rwa005", "1000000000000000", "1000"},
		{"join_logs\\rwa006_all.csv", "price_data_rwa\\rwa006.csv", "rwa006", "1000000000000000", "1000"},
		{"join_logs\\rwa007_all.csv", "price_data_rwa\\rwa007.csv", "rwa007", "1000000000000000", "1000"},
		{"join_logs\\rwa008_all.csv", "price_data_rwa\\rwa008.csv", "rwa008", "1000000000000000", "1000"},
		{"join_logs\\rwa009_all.csv", "price_data_rwa\\rwa009.csv", "rwa009", "1000000000000000", "1000"},
		{"join_logs\\rwa010_all.csv", "price_data_rwa\\rwa010.csv", "rwa010", "1000000000000000", "1000"},
		{"join_logs\\rwa011_all.csv", "price_data_rwa\\rwa011.csv", "rwa011", "1000000000000000", "1000"},
		{"join_logs\\rwa012_all.csv", "price_data_rwa\\rwa012.csv", "rwa012", "1000000000000000", "1000"},
		{"join_logs\\rwa013_all.csv", "price_data_rwa\\rwa013.csv", "rwa013", "1000000000000000", "1000"},
		{"join_logs\\rwa014_all.csv", "price_data_rwa\\rwa014.csv", "rwa014", "1000000000000000", "1000"},
		{"join_logs\\rwa015_all.csv", "price_data_rwa\\rwa015.csv", "rwa015", "1000000000000000", "1000"}
	};

	
	public void combineData(String[][] filePairs, String outputFileName, long start, long end, int distance, int mode) {
		// create the value trees
		Map<String, ValueMap> map = new HashMap<String, ValueMap>();
		for(int i=0;i<filePairs.length;i++) {
			for(int j=0;j<2;j++) {
				String fileName = DATA_DIR + filePairs[i][j];
				ValueMap tree = map.get(fileName);
				if(!filePairs[i][j].equals("")) {
					if(tree == null) {
						System.out.println("processing "+fileName);
						if(j == 0) { // transaction data
							tree = DataProcessor.createBlockTotCollateralMap(fileName, filePairs[i][3]);
						} else if(j == 1) { // 
							tree = DataProcessor.createTimePriceMap(fileName);
						}
						map.put(fileName, tree);
					}
				}
			}
		}
		
		ValueMap timeToBlockMap = DataProcessor.createBlockTimeMap(true);
		
		System.out.println(timeToBlockMap.getMinKey());
		System.out.println(timeToBlockMap.getMaxKey());
		
		// write the output file
		try {
            PrintWriter writer = new PrintWriter(outputFileName);
            String line = "time";
            for(int i=0;i<filePairs.length;i++) {
            	line += "," + filePairs[i][2];
            }
            writer.println(line);

			for(long time = start; time <= end; time += distance) {
				line = time+"";
            	long block = timeToBlockMap.getClosestValueBelow(time);
//            	System.out.println(time+", block "+block);
	            for(int i=0;i<filePairs.length;i++) {
	            	ValueMap collateralMap = map.get(DATA_DIR + filePairs[i][0]);
	            	ValueMap priceMap = map.get(DATA_DIR + filePairs[i][1]);
	            	long scale = Long.parseLong(filePairs[i][4]);
	            	
	    			long collateral = collateralMap.getClosestValueBelow(block);
	    			long price = CENTS_PER_DOLLAR;
	    			if(priceMap != null) {
	    				price = priceMap.getClosestValueBelow(time);
	    			}
	                
//	                System.out.println(collateral+" "+price);
	                
	                DecimalFormat df = new DecimalFormat("#");
	                df.setMaximumFractionDigits(8);
	                
	                if(mode == 0) line += ","+df.format(1.*((collateral * price)/scale)/CENTS_PER_DOLLAR);
	                else if(mode == 1) line += ","+(((collateral * price)/scale)/CENTS_PER_DOLLAR);
	                else line += ","+(collateral/scale);
	            }
	            writer.println(line);
			}
			
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	public void run() {
		combineData(DATA_FILE_PAIRS, System.getProperty("user.dir") + "\\output\\collateral_values_combined.csv", 1572537600, 1707100000, 172800, 0);
		combineData(DATA_FILE_PAIRS, System.getProperty("user.dir") + "\\output\\collateral_int_values_combined.csv", 1572537600, 1707100000, 172800, 1);
		combineData(DATA_FILE_PAIRS, System.getProperty("user.dir") + "\\output\\collateral_tokens_combined.csv", 1572537600, 1707100000, 172800, 2);
	}
	
	public static void main(String[] a) {
		new DataCombiner().run();
	}
}
