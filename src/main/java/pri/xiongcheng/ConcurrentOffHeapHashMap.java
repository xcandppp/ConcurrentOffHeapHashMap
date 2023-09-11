package pri.xiongcheng;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * @author xiongcheng
 * @version 1.1.0
 * <p>
 * V1.1 完成线程安全的ConcurrentOffHeapHashMap
 * <p>
 * TODO：完成红黑树部分内容
 */
public class ConcurrentOffHeapHashMap<K, V> extends AbstractMap<K, V> implements Map<K, V>, Cloneable, Serializable {

    private Node<K, V>[] table;
    private int capacity;
    private final int DEFAULT_CAPACITY = 16;
    private int size;
    private float loadFactor;
    private final float DEFAULT_LOAD_FACTOR = 0.75f;
    private int threshold;

    public ConcurrentOffHeapHashMap() {
        loadFactor = DEFAULT_LOAD_FACTOR;
        capacity = DEFAULT_CAPACITY;
    }

    public ConcurrentOffHeapHashMap(int capacity) {
        this.capacity = capacity;
        loadFactor = DEFAULT_LOAD_FACTOR;
    }

    public ConcurrentOffHeapHashMap(int capacity, float loadFactor) {
        this.capacity = capacity;
        this.loadFactor = loadFactor;
    }

    static long hash(Object key) {
        long h;
        return key == null ? 0 : (h = CityHashUtils.cityHash64(CityHashUtils.toByteArray(key))) ^ h >>> 16;
    }

    @Override
    public ConcurrentOffHeapHashMap<K, V> clone() {
        try {
            ConcurrentOffHeapHashMap<K, V> clone = (ConcurrentOffHeapHashMap<K, V>) super.clone();
            clone.table = (Node<K, V>[]) new Node[this.table.length];
            System.arraycopy(this.table, 0, clone.table, 0, this.table.length);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }


    static class Node<K, V> {
        ByteBuffer keyBuffer;
        ByteBuffer valueBuffer;
        final long hash;
        Node<K, V> next;

        public Node(long hash, K key, V value, Node<K, V> next) {
            this.hash = hash;
            this.next = next;
            this.keyBuffer = ByteBuffer.wrap(CityHashUtils.toByteArray(key));
            this.valueBuffer = ByteBuffer.wrap(CityHashUtils.toByteArray(value));
        }

        public void setValue(V value) {
            valueBuffer = ByteBuffer.wrap(CityHashUtils.toByteArray(value));
        }

        public V getValue() {
            return (V) CityHashUtils.toObject(this.valueBuffer.array());
        }

        public K getKey() {
            return (K) CityHashUtils.toObject(this.keyBuffer.array());
        }

        public final long hash() {
            return this.hash;
        }
    }


    public Node<K, V>[] resize() {
        //TODO 移动红黑树元素

        Node<K, V>[] oldTab = this.table;
        int oldCap = oldTab == null ? 0 : oldTab.length;
        int oldThr = this.threshold;
        int newThr = 0;
        int newCap;
        if (oldCap > 0) {
            if (oldCap >= 1073741824) {
                this.threshold = Integer.MAX_VALUE;
                return oldTab;
            }

            if ((newCap = oldCap << 1) < 1073741824 && oldCap >= 16) {
                newThr = oldThr << 1;
            }
        } else if (oldThr > 0) {
            newCap = oldThr;
        } else {
            newCap = 16;
            newThr = 12;
        }

        if (newThr == 0) {
            float ft = (float) newCap * this.loadFactor;
            newThr = newCap < 1073741824 && ft < 1.07374182E9F ? (int) ft : Integer.MAX_VALUE;
        }

        this.threshold = newThr;
        Node<K, V>[] newTab = new Node[newCap];
        this.table = newTab;
        if (oldTab != null) {
            for (int j = 0; j < oldCap; ++j) {
                Node e;
                if ((e = oldTab[j]) != null) {
                    oldTab[j] = null;
                    if (e.next == null) {
                        newTab[(int) (e.hash & newCap - 1)] = e;
//                    } else if (e instanceof TreeNode) {
//                        ((TreeNode)e).split(this, newTab, j, oldCap);
                    } else {
                        Node<K, V> loHead = null;
                        Node<K, V> loTail = null;
                        Node<K, V> hiHead = null;
                        Node<K, V> hiTail = null;

                        Node next;
                        do {
                            next = e.next;
                            if ((e.hash & oldCap) == 0) {
                                if (loTail == null) {
                                    loHead = e;
                                } else {
                                    loTail.next = e;
                                }

                                loTail = e;
                            } else {
                                if (hiTail == null) {
                                    hiHead = e;
                                } else {
                                    hiTail.next = e;
                                }

                                hiTail = e;
                            }

                            e = next;
                        } while (next != null);

                        if (loTail != null) {
                            loTail.next = null;
                            newTab[j] = loHead;
                        }

                        if (hiTail != null) {
                            hiTail.next = null;
                            newTab[j + oldCap] = hiHead;
                        }
                    }
                }
            }
        }

        return newTab;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public V get(Object key) {
        Node<K, V> e;
        return (e = this.getNode(key)) == null ? null : e.getValue();
    }

    final Node<K, V> getNode(Object key) {
        int n = table.length;
        long hash = CityHashUtils.cityHash64(CityHashUtils.toByteArray(key));
        int i = (int) (table.length - 1 & hash);
        Node<K, V> first = table[i];
        if (table != null && n > 0 && first != null) {
            Object k;
            if (first.hash == hash && ((k = first.getKey()) == key || key != null && key.equals(k))) {
                return first;
            }
        }

        return null;
    }

    @Override
    public V put(K key, V value) {
        putValue(CityHashUtils.cityHash64(CityHashUtils.toByteArray(key)), key, value);
        ++this.size;
        return null;
    }

    final V putValue(long hash, K key, V value) {
        //TODO 链表转红黑树实现
        Node<K, V>[] tab;
        int n;
        if ((tab = this.table) == null || (n = tab.length) == 0) {
            n = (tab = this.resize()).length;
        }
        int i = (int) (n - 1 & hash);
        Object p = tab[i];
        if (p == null) {
            tab[i] = this.newNode(hash, key, value, (Node<K, V>) null);
        } else {
            synchronized (p) {
                Object e;
                Object k;
                if (((Node) p).hash == hash && ((k = ((Node) p).getKey()) == key || key != null && key.equals(k))) {
                    e = p;
//            } else if (p instanceof TreeNode) {
//                e = ((TreeNode)p).putTreeVal(this, tab, hash, key, value);
                } else {
//                int binCount = 0;

                    while (true) {
                        if ((e = ((Node) p).next) == null) {
                            ((Node) p).next = this.newNode(hash, key, value, (Node) null);
//                        if (binCount >= 7) {
//                            this.treeifyBin(tab, hash);
//                        }
                            break;
                        }

                        if (((Node) e).hash == hash && ((k = ((Node) e).getKey()) == key || key != null && key.equals(k))) {
                            break;
                        }

                        p = e;
//                    ++binCount;
                    }
                }

                if (e != null) {
                    V oldValue = (V) ((Node) e).getValue();
                    ((Node) e).setValue(value);
                    return oldValue;
                }
            }

        }

        if (++this.size > this.threshold) {
            this.resize();
        }

        return null;
    }

    private Node<K, V> newNode(long hash, K key, V value, Node<K, V> next) {
        return new Node<K, V>(hash, key, value, next);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    @Override
    public void clear() {
        for (Node<K, V> tab : table) {
            if (tab != null) {
                tab.keyBuffer.clear();
                tab.valueBuffer.clear();
            }
        }
        this.table = null;
        this.size = 0;
    }
}