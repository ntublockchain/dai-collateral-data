package dai_collateral;

public class ValueMap {

	private long[] keys;
	private long[] values;
	private int length;
	private int last;
	private long minVal;
	
	public ValueMap(Long[] keys, Long[] values) {
		this(keys, values, 0);
	}
	
	public ValueMap(Long[] keys, Long[] values, long minVal) {
		this(keys, values, minVal, false);
	}
	
	public ValueMap(Long[] keys, Long[] values, long minVal, boolean reverse) {
		length = keys.length;
		
		this.keys = new long[length];
		this.values = new long[length];
		
		for(int i=0;i<length;i++) {
			if(reverse) {
				this.keys[i] = keys[length - i - 1];
				this.values[i] = values[length - i - 1];
			} else {
				this.keys[i] = keys[i];
				this.values[i] = values[i];
			}
		}
		
		this.minVal = minVal;
		
		last = 0;
	}
	
	public long getMinKey() {
		return keys[0];
	}
	
	public long getMaxKey() {
		return keys[length-1];
	}
	
	public long getClosestValueBelow(long v) {
		if(length == 0) return 0;
		if(keys[last] > v) last = 0;
		
		for(int i=last;i<length;i++) {
			if(keys[i] > v) {
				if(i == 0) {
					last = 0;
					return minVal;
				} else {
					last = i - 1;
					return values[last];
				}
			}
		}
		
		last = length - 1;
		return values[last];
	}
	
	public static void main(String[] a) {
		// tests
		Long[] b = {(long) 100, (long) 200, (long) 300, (long) 400, (long) 500};
		Long[] c = {(long) 1, (long) 20, (long) 40, (long) 30, (long) 5};
		ValueMap tree = new ValueMap(b ,c);
		
		System.out.println("test results:");
		System.out.println(tree.getClosestValueBelow(50)+" vs 1");
		System.out.println(tree.getClosestValueBelow(150)+" vs 1");
		System.out.println(tree.getClosestValueBelow(250)+" vs 20");
		System.out.println(tree.getClosestValueBelow(350)+" vs 40");
		System.out.println(tree.getClosestValueBelow(450)+" vs 30");
		System.out.println(tree.getClosestValueBelow(400)+" vs 30");
		System.out.println(tree.getClosestValueBelow(300)+" vs 40");
		System.out.println(tree.getClosestValueBelow(100)+" vs 1");
		System.out.println(tree.getClosestValueBelow(550)+" vs 5");
		System.out.println(tree.getClosestValueBelow(-50)+" vs 0");
		System.out.println(tree.getClosestValueBelow(300)+" vs 40");
		System.out.println(tree.getClosestValueBelow(300)+" vs 40");
		System.out.println(tree.getClosestValueBelow(301)+" vs 40");
		System.out.println(tree.getClosestValueBelow(299)+" vs 20");
	}
}
