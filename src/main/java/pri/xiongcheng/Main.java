package pri.xiongcheng;

/**
 * @author xiongcheng
 */
public class Main {
    public static void main(String[] args) {
        ConcurrentOffHeapHashMap<String,String> map = new ConcurrentOffHeapHashMap<>();
        map.put("key1","value1");
        map.put("key2","value2");
        map.put("key3","value3");
        map.put("key4","value4");
        map.put("key5","value5");
        map.put("key6","value6");
        map.put("key7","value7");
        map.put("key8","value8");
        map.put("key9","value9");
        map.put("key10","value10");
        map.put("key11","value11");
        map.put("key12","value12");
        System.out.println(map.get("key1"));
        map.clear();
    }
}