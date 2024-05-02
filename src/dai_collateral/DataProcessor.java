package dai_collateral;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DataProcessor {
	
	final static String JOIN_ID = 	"0x3b4da69f00000000000000000000000000000000000000000000000000000000";
	final static String JOIN_ID_2 =	"0xd14b1e4b00000000000000000000000000000000000000000000000000000000";
	final static String EXIT_ID =	"0xef693bed00000000000000000000000000000000000000000000000000000000";
	
	final static String TIMESTAMP_DATA = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\bigquery\\analysis_data\\block_timestamps.csv";
	
	final static String ETH_A_DATA = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\bigquery\\analysis_data\\join_logs\\eth_a_all.csv";
	
	final static String ETH_PRICE_DATA = "C:\\Users\\dani3\\Dropbox\\utwente backup\\research\\geschreven_tekst\\2023_ntu\\2024_dai_collateral\\bigquery\\analysis_data\\price_data_gemini\\eth_usd.csv";
	
	final static String WEI_PER_ETH = "1000000000000000";

	public static ValueMap createBlockTimeMap(boolean reverse) {
		BufferedReader reader;
		
		List<Long> blocks = new ArrayList<Long>();
		List<Long> timestamps = new ArrayList<Long>();
		
		try {
			reader = new BufferedReader(new FileReader(TIMESTAMP_DATA));
			String line = reader.readLine(); // first line denotes the columns names

			while ((line = reader.readLine()) != null) {
				String[] lineData = line.split(",");
				long blockNumber = Long.parseLong(lineData[0]);
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.000000 z");
				ZonedDateTime zonedDateTime = ZonedDateTime.parse(lineData[1], formatter);
				
				blocks.add(blockNumber);
				timestamps.add(zonedDateTime.toEpochSecond());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(reverse) return new ValueMap(timestamps.toArray(new Long[0]), blocks.toArray(new Long[0]));
		return new ValueMap(blocks.toArray(new Long[0]), timestamps.toArray(new Long[0]));
	}
	
	public static ValueMap createTimePriceMap(String fileName) {
		BufferedReader reader;
		
		List<Long> timestamps = new ArrayList<Long>();
		List<Long> prices = new ArrayList<Long>();
		
		BigInteger millisPerSecond = new BigInteger("1000");
		
		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine(); // first line denotes the source
			line = reader.readLine(); // second line denotes the columns names

			while ((line = reader.readLine()) != null) {
				String[] lineData = line.split(",");
				BigInteger timeMillis = new BigInteger(lineData[0]);
//				System.out.println(timeMillis);
				long timestamp = timeMillis.divide(millisPerSecond).longValue();
				if(timestamp < 1000000000) { // before 2018-06-27 11:00:00, timestamps are in seconds, afterwards in milliseconds 
					timestamp *= 1000;
				}
				
				long priceOpen = (long) (Double.parseDouble(lineData[3]) * 100);
				long priceClose = (long) (Double.parseDouble(lineData[6]) * 100);
				long priceAverage = (priceOpen + priceClose)/2;
				
//				System.out.println(timestamp+" "+priceAverage);
				
				timestamps.add(timestamp);
				prices.add(priceAverage);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new ValueMap(timestamps.toArray(new Long[0]), prices.toArray(new Long[0]), 0, true);
	}
	
	public static BigInteger hex2decimal(String s) {
	    String digits = "0123456789ABCDEF";
	    String s2 = s.toUpperCase();
	    BigInteger val = new BigInteger("0");
	    BigInteger base = new BigInteger("1");
	    BigInteger hex = new BigInteger("16");
	    for (int i = s2.length()-1; i >= 0; i--) {
	        char c = s2.charAt(i);
	        int d = digits.indexOf(c);
	        BigInteger term = base.multiply(new BigInteger(""+d));
	        val = val.add(term);
	        base = base.multiply(hex);
	    }
	    return val;
	}
	
	public static String filterHexString(String s) {
		int j = -1;
		for(int i=0;i<s.length();i++) {
			char c = s.charAt(i);
			if(c != '0' && c != 'x') {
				j = i;
				break;
			}
		}
		if(j >= 0) {
			return s.substring(j);
		}
		return "0";
	}
	
	public static ValueMap createBlockTotCollateralMap(String fileName, String scaleFactorString) {
		BufferedReader reader;
		BigInteger scaleFactor = new BigInteger(scaleFactorString);
		
		List<Long> blocks = new ArrayList<Long>();
		List<Long> tots = new ArrayList<Long>();

		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line;
			
			int n = 0;
			BigInteger totalWei = new BigInteger("0");
			BigInteger totalAbsoluteWei = new BigInteger("0");
			BigInteger inverse = new BigInteger("-1");
			long totalEth = 0;

			while ((line = reader.readLine()) != null) {
				n++;
				if(n>1) { // ignore the line with column names
					String[] lineData = line.split(",");
					long block = Long.parseLong(lineData[0]);
					int type = -1;
					if(lineData[1].equals(JOIN_ID) || lineData[1].equals(JOIN_ID_2)) {
						type = 0;
					} else if(lineData[1].equals(EXIT_ID)) {
						type = 1;
					}
					if(type >= 0) {
						blocks.add(block);
						BigInteger value = hex2decimal(filterHexString(lineData[2].trim()));
						totalAbsoluteWei = totalAbsoluteWei.add(value);
						if(type == 1) value = value.multiply(inverse);
						totalWei = totalWei.add(value);
						
						long ethChange = value.divide(scaleFactor).longValue();
						totalEth += ethChange;
						tots.add(totalEth);
						
//						System.out.println(totalWei);
					}
				}
			}
					
//			System.out.println(n+" transfers");
//			System.out.println(totalAbsoluteWei+" Wei transfered");
//			System.out.println(totalWei+" net Wei at end time");
//			System.out.println(totalEth+" net ETH at end time");
//			
//			BigInteger priceOfEth = new BigInteger("2307");
//			BigInteger collateralValue = new BigInteger(""+totalEth).multiply(priceOfEth);
//			System.out.println(collateralValue+" dollars worth of collateral at end time");

			reader.close();
			
			return new ValueMap(blocks.toArray(new Long[0]), tots.toArray(new Long[0]));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public void createTimeTokensGraph(ValueMap blockTimeMap, ValueMap blockTotCollateralMap, String fileName) {
		long minKey = blockTotCollateralMap.getMinKey();
		long maxKey = blockTotCollateralMap.getMaxKey();
		
		int granularity = 10000; // determines the number of points
		
		// round to 1000s
		minKey = (long) Math.floor(minKey/granularity) * granularity;
		maxKey = (long) Math.floor(maxKey/granularity) * granularity;
		
		int numPoints = (int) (maxKey - minKey)/granularity + 1;
		
		System.out.println(minKey+" to "+maxKey+": "+numPoints+" points");
		
		try {
            PrintWriter writer = new PrintWriter(fileName);
            writer.println("block,timestamp,collateral");
            
    		for(int i=0;i<=numPoints;i++) {
    			long block = i * granularity + minKey;
    			writer.println(block+","+blockTimeMap.getClosestValueBelow(block)+","+blockTotCollateralMap.getClosestValueBelow(block));
    		}
            
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void createTimeDollarsGraph(ValueMap blockTimeMap, ValueMap blockTotCollateralMap, ValueMap timePriceMap, String fileName) {
		long minKey = blockTotCollateralMap.getMinKey();
		long maxKey = blockTotCollateralMap.getMaxKey();
		
		int granularity = 10000; // determines the number of points
		
		// round to 1000s
		minKey = (long) Math.floor(minKey/granularity) * granularity;
		maxKey = (long) Math.floor(maxKey/granularity) * granularity;
		
		int numPoints = (int) (maxKey - minKey)/granularity + 1;
		
		System.out.println(minKey+" to "+maxKey+": "+numPoints+" points");
		
		try {
            PrintWriter writer = new PrintWriter(fileName);
            writer.println("block,timestamp,collateral,collvalue");
            
            
    		for(int i=0;i<=numPoints;i++) {
    			long block = i * granularity + minKey;
    			long time = blockTimeMap.getClosestValueBelow(block);
    			long collateral = blockTotCollateralMap.getClosestValueBelow(block);
                long price = timePriceMap.getClosestValueBelow(time)/100;
    			writer.println(block+","+time+","+collateral+","+(price*collateral));
    		}
            
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void createTimeEthPriceGraph(ValueMap blockTimeMap, ValueMap blockTotCollateralMap, ValueMap timePriceMap, String fileName) {
		long minKey = blockTotCollateralMap.getMinKey();
		long maxKey = blockTotCollateralMap.getMaxKey();
		
		int granularity = 10000; // determines the number of points
		
		// round to 1000s
		minKey = (long) Math.floor(minKey/granularity) * granularity;
		maxKey = (long) Math.floor(maxKey/granularity) * granularity;
		
		int numPoints = (int) (maxKey - minKey)/granularity + 1;
		
		System.out.println(minKey+" to "+maxKey+": "+numPoints+" points");
		
		try {
            PrintWriter writer = new PrintWriter(fileName);
            writer.println("block,timestamp,price");
            
            
    		for(int i=0;i<=numPoints;i++) {
    			long block = i * granularity + minKey;
    			long time = blockTimeMap.getClosestValueBelow(block);
                long price = timePriceMap.getClosestValueBelow(time)/100;
    			writer.println(block+","+time+","+price);
    		}
            
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void processAllVaults() {
		
	}
	
	public void run() {
		ValueMap blockTimeMap = createBlockTimeMap(false);
		ValueMap blockTotCollateralMap = createBlockTotCollateralMap(ETH_A_DATA, WEI_PER_ETH);
		ValueMap timePriceMap = createTimePriceMap(ETH_PRICE_DATA);
		
		createTimeTokensGraph(blockTimeMap, blockTotCollateralMap, System.getProperty("user.dir")+"\\output\\timeTokensGraph.csv");
		createTimeDollarsGraph(blockTimeMap, blockTotCollateralMap, timePriceMap, System.getProperty("user.dir")+"\\output\\timeDollarsGraph.csv");
		createTimeEthPriceGraph(blockTimeMap, blockTotCollateralMap, timePriceMap, System.getProperty("user.dir")+"\\output\\timeEthPriceGraph.csv");
	}
	
	public static void main(String[] a) {
		new DataProcessor().run();
	}

}
