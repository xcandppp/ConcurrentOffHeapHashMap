package pri.xiongcheng;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xiongcheng
 * @version 1.2.0
 * <p>
 * V1.2 完成红黑树部分内容，优化线程安全机制
 */
public class ConcurrentOffHeapHashMap<K, V> extends AbstractMap<K, V> implements Cloneable, Serializable {

    private volatile Node<K, V>[] table;
    private int capacity;
    private final int DEFAULT_CAPACITY = 16;
    private final AtomicInteger size = new AtomicInteger(0);
    private float loadFactor;
    private final float DEFAULT_LOAD_FACTOR = 0.75f;
    private volatile int threshold;
    private static final int MIN_TREEIFY_CAPACITY = 64;
    private static final int TREEIFY_THRESHOLD = 8;
    private static final int UNTREEIFY_THRESHOLD = 6;

    public ConcurrentOffHeapHashMap() {
        loadFactor = DEFAULT_LOAD_FACTOR;
        capacity = DEFAULT_CAPACITY;
        threshold = (int) (capacity * loadFactor);
    }

    public ConcurrentOffHeapHashMap(int capacity) {
        this.capacity = capacity;
        loadFactor = DEFAULT_LOAD_FACTOR;
        threshold = (int) (capacity * loadFactor);
    }

    public ConcurrentOffHeapHashMap(int capacity, float loadFactor) {
        this.capacity = capacity;
        this.loadFactor = loadFactor;
        threshold = (int) (capacity * loadFactor);
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

    /**
     * 红黑树节点类
     */
    static final class TreeNode<K, V> extends Node<K, V> {
        TreeNode<K, V> parent;
        TreeNode<K, V> left;
        TreeNode<K, V> right;
        TreeNode<K, V> prev;
        boolean red;

        TreeNode(long hash, K key, V value, Node<K, V> next, TreeNode<K, V> parent) {
            super(hash, key, value, next);
            this.parent = parent;
            this.red = false;
        }

        /**
         * 返回根节点
         */
        final TreeNode<K, V> root() {
            for (TreeNode<K, V> r = this, p; ; ) {
                if ((p = r.parent) == null) {
                    return r;
                }
                r = p;
            }
        }

        /**
         * 确保根节点是给定的节点
         */
        static <K, V> void moveRootToFront(Node<K, V>[] tab, TreeNode<K, V> root) {
            int n;
            if (root != null && tab != null && (n = tab.length) > 0) {
                int index = (n - 1) & (int) root.hash;
                TreeNode<K, V> first = (TreeNode<K, V>) tab[index];
                if (root != first) {
                    Node<K, V> rn;
                    tab[index] = root;
                    TreeNode<K, V> rp = root.prev;
                    if ((rn = root.next) != null) {
                        ((TreeNode<K, V>) rn).prev = rp;
                    }
                    if (rp != null) {
                        rp.next = rn;
                    }
                    if (first != null) {
                        first.prev = root;
                    }
                    root.next = first;
                    root.prev = null;
                }
                assert checkInvariants(root);
            }
        }

        /**
         * 查找节点
         */
        final TreeNode<K, V> find(long h, Object k, Class<?> kc) {
            TreeNode<K, V> p = this;
            do {
                int ph, dir;
                K pk;
                TreeNode<K, V> pl = p.left, pr = p.right, q;
                if ((ph = (int) p.hash) > (int) h) {
                    p = pl;
                } else if (ph < (int) h) {
                    p = pr;
                } else if ((pk = p.getKey()) == k || (k != null && k.equals(pk))) {
                    return p;
                } else if (pl == null) {
                    p = pr;
                } else if (pr == null) {
                    p = pl;
                } else if ((kc != null ||
                        (kc = comparableClassFor(k)) != null) &&
                        (dir = compareComparables(kc, k, pk)) != 0) {
                    p = (dir < 0) ? pl : pr;
                } else if ((q = pr.find(h, k, kc)) != null) {
                    return q;
                } else {
                    p = pl;
                }
            } while (p != null);
            return null;
        }

        /**
         * 获取树节点
         */
        final TreeNode<K, V> getTreeNode(long h, Object k) {
            return ((parent != null) ? root() : this).find(h, k, null);
        }

        /**
         * 比较两个键的大小
         */
        static int tieBreakOrder(Object a, Object b) {
            int d;
            if (a == null || b == null ||
                    (d = a.getClass().getName().compareTo(b.getClass().getName())) == 0) {
                d = (System.identityHashCode(a) <= System.identityHashCode(b) ?
                        -1 : 1);
            }
            return d;
        }

        /**
         * 将链表转换为红黑树
         */
        final void treeify(Node<K, V>[] tab) {
            TreeNode<K, V> root = null;
            for (TreeNode<K, V> x = this, next; x != null; x = next) {
                next = (TreeNode<K, V>) x.next;
                x.left = x.right = null;
                if (root == null) {
                    x.parent = null;
                    x.red = false;
                    root = x;
                } else {
                    K k = x.getKey();
                    long h = x.hash;
                    Class<?> kc = null;
                    for (TreeNode<K, V> p = root; ; ) {
                        int dir, ph;
                        K pk = p.getKey();
                        if ((ph = (int) p.hash) > (int) h) {
                            dir = -1;
                        } else if (ph < (int) h) {
                            dir = 1;
                        } else if ((kc == null &&
                                (kc = comparableClassFor(k)) == null) ||
                                (dir = compareComparables(kc, k, pk)) == 0) {
                            dir = tieBreakOrder(k, pk);
                        }

                        TreeNode<K, V> xp = p;
                        if ((p = (dir <= 0) ? p.left : p.right) == null) {
                            x.parent = xp;
                            if (dir <= 0) {
                                xp.left = x;
                            } else {
                                xp.right = x;
                            }
                            root = balanceInsertion(root, x);
                            break;
                        }
                    }
                }
            }
            moveRootToFront(tab, root);
        }

        /**
         * 将红黑树转换为链表
         */
        final Node<K, V> untreeify(ConcurrentOffHeapHashMap<K, V> map) {
            Node<K, V> hd = null, tl = null;
            for (Node<K, V> q = this; q != null; q = q.next) {
                Node<K, V> p = map.replacementNode(q, null);
                if (tl == null) {
                    hd = p;
                } else {
                    tl.next = p;
                }
                tl = p;
            }
            return hd;
        }

        /**
         * 插入树节点
         */
        final TreeNode<K, V> putTreeVal(ConcurrentOffHeapHashMap<K, V> map,
                                        Node<K, V>[] tab,
                                        long h, K k, V v) {
            Class<?> kc = null;
            boolean searched = false;
            TreeNode<K, V> root = (parent != null) ? root() : this;
            for (TreeNode<K, V> p = root; ; ) {
                int dir, ph;
                K pk;
                if ((ph = (int) p.hash) > (int) h) {
                    dir = -1;
                } else if (ph < (int) h) {
                    dir = 1;
                } else if ((pk = p.getKey()) == k || (k != null && k.equals(pk))) {
                    return p;
                } else if ((kc == null &&
                        (kc = comparableClassFor(k)) == null) ||
                        (dir = compareComparables(kc, k, pk)) == 0) {
                    if (!searched) {
                        TreeNode<K, V> q, ch;
                        searched = true;
                        if (((ch = p.left) != null &&
                                (q = ch.find(h, k, kc)) != null) ||
                                ((ch = p.right) != null &&
                                        (q = ch.find(h, k, kc)) != null)) {
                            return q;
                        }
                    }
                    dir = tieBreakOrder(k, pk);
                }

                TreeNode<K, V> xp = p;
                if ((p = (dir <= 0) ? p.left : p.right) == null) {
                    Node<K, V> xpn = xp.next;
                    TreeNode<K, V> x = map.newTreeNode(h, k, v, xpn, xp);
                    if (dir <= 0) {
                        xp.left = x;
                    } else {
                        xp.right = x;
                    }
                    xp.next = x;
                    x.parent = x.prev = xp;
                    if (xpn != null) {
                        ((TreeNode<K, V>) xpn).prev = x;
                    }
                    moveRootToFront(tab, balanceInsertion(root, x));
                    return null;
                }
            }
        }

        /**
         * 删除树节点
         */
        final boolean removeTreeNode(ConcurrentOffHeapHashMap<K, V> map,
                                      Node<K, V>[] tab,
                                      boolean movable) {
            int n;
            if (tab == null || (n = tab.length) == 0) {
                return false;
            }
            int index = (n - 1) & (int) hash;
            TreeNode<K, V> first = (TreeNode<K, V>) tab[index], root = first, rl;
            TreeNode<K, V> succ = (TreeNode<K, V>) next, pred = prev;
            if (pred == null) {
                tab[index] = first = succ;
            } else {
                pred.next = succ;
            }
            if (succ != null) {
                succ.prev = pred;
            }
            if (first == null) {
                return false;
            }
            if (root.parent != null) {
                root = root.root();
            }
            if (root == null || root.right == null ||
                    (rl = root.left) == null || rl.left == null) {
                tab[index] = first.untreeify(map);
                return true;
            }
            TreeNode<K, V> p = this, pl = left, pr = right, replacement;
            if (pl != null && pr != null) {
                TreeNode<K, V> s = pr, sl;
                while ((sl = s.left) != null)
                    s = sl;
                boolean c = s.red;
                s.red = p.red;
                p.red = c;
                TreeNode<K, V> sr = s.right;
                TreeNode<K, V> pp = p.parent;
                if (s == pr) {
                    p.parent = s;
                    s.right = p;
                } else {
                    TreeNode<K, V> sp = s.parent;
                    if ((p.parent = sp) != null) {
                        if (s == sp.left) {
                            sp.left = p;
                        } else {
                            sp.right = p;
                        }
                    }
                    if ((s.right = pr) != null) {
                        pr.parent = s;
                    }
                }
                p.left = null;
                if ((p.right = sr) != null) {
                    sr.parent = p;
                }
                if ((s.left = pl) != null) {
                    pl.parent = s;
                }
                if ((s.parent = pp) == null) {
                    root = s;
                } else if (p == pp.left) {
                    pp.left = s;
                } else {
                    pp.right = s;
                }
                if (sr != null) {
                    replacement = sr;
                } else {
                    replacement = p;
                }
            } else if (pl != null) {
                replacement = pl;
            } else if (pr != null) {
                replacement = pr;
            } else {
                replacement = p;
            }
            if (replacement != p) {
                TreeNode<K, V> pp = replacement.parent = p.parent;
                if (pp == null) {
                    root = replacement;
                } else if (p == pp.left) {
                    pp.left = replacement;
                } else {
                    pp.right = replacement;
                }
                p.left = p.right = p.parent = null;
            }

            TreeNode<K, V> r = p.red ? root : balanceDeletion(root, replacement);

            if (replacement == p) {
                TreeNode<K, V> pp = p.parent;
                p.parent = null;
                if (pp != null) {
                    if (p == pp.left) {
                        pp.left = null;
                    } else if (p == pp.right) {
                        pp.right = null;
                    }
                }
            }
            if (movable) {
                moveRootToFront(tab, r);
            }
            return true;
        }

        /**
         * 分割树节点（用于resize）
         */
        final void split(ConcurrentOffHeapHashMap<K, V> map, Node<K, V>[] tab, int index, int bit) {
            TreeNode<K, V> b = this;
            TreeNode<K, V> loHead = null, loTail = null;
            TreeNode<K, V> hiHead = null, hiTail = null;
            int lc = 0, hc = 0;
            for (TreeNode<K, V> e = b, next; e != null; e = next) {
                next = (TreeNode<K, V>) e.next;
                e.next = null;
                if (((int) e.hash & bit) == 0) {
                    if ((e.prev = loTail) == null) {
                        loHead = e;
                    } else {
                        loTail.next = e;
                    }
                    loTail = e;
                    ++lc;
                } else {
                    if ((e.prev = hiTail) == null) {
                        hiHead = e;
                    } else {
                        hiTail.next = e;
                    }
                    hiTail = e;
                    ++hc;
                }
            }

            if (loHead != null) {
                if (lc <= UNTREEIFY_THRESHOLD) {
                    tab[index] = loHead.untreeify(map);
                } else {
                    tab[index] = loHead;
                    if (hiHead != null) {
                        loHead.treeify(tab);
                    }
                }
            }
            if (hiHead != null) {
                if (hc <= UNTREEIFY_THRESHOLD) {
                    tab[index + bit] = hiHead.untreeify(map);
                } else {
                    tab[index + bit] = hiHead;
                    if (loHead != null) {
                        hiHead.treeify(tab);
                    }
                }
            }
        }

        /**
         * 左旋
         */
        static <K, V> TreeNode<K, V> rotateLeft(TreeNode<K, V> root,
                                                  TreeNode<K, V> p) {
            TreeNode<K, V> r, pp, rl;
            if (p != null && (r = p.right) != null) {
                if ((rl = p.right = r.left) != null) {
                    rl.parent = p;
                }
                if ((pp = r.parent = p.parent) == null) {
                    (root = r).red = false;
                } else if (pp.left == p) {
                    pp.left = r;
                } else {
                    pp.right = r;
                }
                r.left = p;
                p.parent = r;
            }
            return root;
        }

        /**
         * 右旋
         */
        static <K, V> TreeNode<K, V> rotateRight(TreeNode<K, V> root,
                                                   TreeNode<K, V> p) {
            TreeNode<K, V> l, pp, lr;
            if (p != null && (l = p.left) != null) {
                if ((lr = p.left = l.right) != null) {
                    lr.parent = p;
                }
                if ((pp = l.parent = p.parent) == null) {
                    (root = l).red = false;
                } else if (pp.right == p) {
                    pp.right = l;
                } else {
                    pp.left = l;
                }
                l.right = p;
                p.parent = l;
            }
            return root;
        }

        /**
         * 插入后平衡
         */
        static <K, V> TreeNode<K, V> balanceInsertion(TreeNode<K, V> root,
                                                        TreeNode<K, V> x) {
            x.red = true;
            for (TreeNode<K, V> xp, xpp, xppl, xppr; ; ) {
                if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                } else if (!xp.red || (xpp = xp.parent) == null) {
                    return root;
                }
                if (xp == (xppl = xpp.left)) {
                    if ((xppr = xpp.right) != null && xppr.red) {
                        xppr.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    } else {
                        if (x == xp.right) {
                            root = rotateLeft(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateRight(root, xpp);
                            }
                        }
                    }
                } else {
                    if (xppl != null && xppl.red) {
                        xppl.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    } else {
                        if (x == xp.left) {
                            root = rotateRight(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateLeft(root, xpp);
                            }
                        }
                    }
                }
            }
        }

        /**
         * 删除后平衡
         */
        static <K, V> TreeNode<K, V> balanceDeletion(TreeNode<K, V> root,
                                                       TreeNode<K, V> x) {
            for (TreeNode<K, V> xp, xpl, xpr; ; ) {
                if (x == null || x == root) {
                    return root;
                } else if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                } else if (x.red) {
                    x.red = false;
                    return root;
                } else if ((xpl = xp.left) == x) {
                    if ((xpr = xp.right) != null && xpr.red) {
                        xpr.red = false;
                        xp.red = true;
                        root = rotateLeft(root, xp);
                        xpr = (xp = x.parent) == null ? null : xp.right;
                    }
                    if (xpr == null) {
                        x = xp;
                    }
                    else {
                        TreeNode<K, V> sl = xpr.left, sr = xpr.right;
                        if ((sr == null || !sr.red) &&
                                (sl == null || !sl.red)) {
                            xpr.red = true;
                            x = xp;
                        } else {
                            if (sr == null || !sr.red) {
                                if (sl != null) {
                                    sl.red = false;
                                }
                                xpr.red = true;
                                root = rotateRight(root, xpr);
                                xpr = (xp = x.parent) == null ?
                                        null : xp.right;
                            }
                            if (xpr != null) {
                                xpr.red = (xp == null) ? false : xp.red;
                                if ((sr = xpr.right) != null) {
                                    sr.red = false;
                                }
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateLeft(root, xp);
                            }
                            x = root;
                        }
                    }
                } else {
                    if (xpl != null && xpl.red) {
                        xpl.red = false;
                        xp.red = true;
                        root = rotateRight(root, xp);
                        xpl = (xp = x.parent) == null ? null : xp.left;
                    }
                    if (xpl == null) {
                        x = xp;
                    } else {
                        TreeNode<K, V> sl = xpl.left, sr = xpl.right;
                        if ((sl == null || !sl.red) &&
                                (sr == null || !sr.red)) {
                            xpl.red = true;
                            x = xp;
                        } else {
                            if (sl == null || !sl.red) {
                                if (sr != null) {
                                    sr.red = false;
                                }
                                xpl.red = true;
                                root = rotateLeft(root, xpl);
                                xpl = (xp = x.parent) == null ?
                                        null : xp.left;
                            }
                            if (xpl != null) {
                                xpl.red = (xp == null) ? false : xp.red;
                                if ((sl = xpl.left) != null) {
                                    sl.red = false;
                                }
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateRight(root, xp);
                            }
                            x = root;
                        }
                    }
                }
            }
        }

        /**
         * 检查红黑树不变性
         */
        static <K, V> boolean checkInvariants(TreeNode<K, V> t) {
            TreeNode<K, V> tp = t.parent, tl = t.left, tr = t.right,
                    tb = t.prev, tn = (TreeNode<K, V>) t.next;
            if (tb != null && tb.next != t) {
                return false;
            }
            if (tn != null && tn.prev != t) {
                return false;
            }
            if (tp != null && t != tp.left && t != tp.right) {
                return false;
            }
            if (tl != null && (tl.parent != t || tl.hash > (int) t.hash)) {
                return false;
            }
            if (tr != null && (tr.parent != t || tr.hash < (int) t.hash)) {
                return false;
            }
            if (t.red && tl != null && tl.red && tr != null && tr.red) {
                return false;
            }
            if (tl != null && !checkInvariants(tl)) {
                return false;
            }
            if (tr != null && !checkInvariants(tr)) {
                return false;
            }
            return true;
        }
    }

    /**
     * 工具方法：获取可比较类
     */
    static Class<?> comparableClassFor(Object x) {
        if (x instanceof Comparable) {
            Class<?> c;
            Type[] ts;
            Type[] p;
            if ((c = x.getClass()) == String.class) {
                return c;
            }
            if ((ts = c.getGenericInterfaces()) != null) {
                for (Type t : ts) {
                    if ((t instanceof ParameterizedType) &&
                            ((p = ((ParameterizedType) t).getActualTypeArguments()) != null &&
                                    p.length == 1 && p[0] == c)) {
                        return c;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 工具方法：比较可比较对象
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static int compareComparables(Class<?> kc, Object k, Object x) {
        return (x == null || x.getClass() != kc ? 0 :
                ((Comparable) k).compareTo(x));
    }


    public Node<K, V>[] resize() {
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
            newCap = DEFAULT_CAPACITY;
            newThr = (int) (DEFAULT_CAPACITY * loadFactor);
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
                Node<K, V> e;
                if ((e = oldTab[j]) != null) {
                    oldTab[j] = null;
                    if (e.next == null) {
                        newTab[(int) (e.hash & (newCap - 1))] = e;
                    } else if (e instanceof TreeNode) {
                        ((TreeNode<K, V>) e).split(this, newTab, j, oldCap);
                    } else {
                        Node<K, V> loHead = null;
                        Node<K, V> loTail = null;
                        Node<K, V> hiHead = null;
                        Node<K, V> hiTail = null;

                        Node<K, V> next;
                        do {
                            next = e.next;
                            if (((int) e.hash & oldCap) == 0) {
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
        return removeNode(key);
    }

    final V removeNode(Object key) {
        Node<K, V>[] tab;
        Node<K, V> p;
        int n, index;
        long hash = hash(key);
        if ((tab = table) != null && (n = tab.length) > 0 &&
                (p = tab[index = (n - 1) & (int) hash]) != null) {
            Node<K, V> node = null, e;
            K k;
            if (p.hash == hash &&
                    ((k = p.getKey()) == key || (key != null && key.equals(k)))) {
                node = p;
            } else if ((e = p.next) != null) {
                if (p instanceof TreeNode) {
                    node = ((TreeNode<K, V>) p).getTreeNode(hash, key);
                }
                else {
                    do {
                        if (e.hash == hash &&
                                ((k = e.getKey()) == key ||
                                        (key != null && key.equals(k)))) {
                            node = e;
                            break;
                        }
                        p = e;
                    } while ((e = e.next) != null);
                }
            }
            if (node != null) {
                V oldValue = node.getValue();
                if (node instanceof TreeNode) {
                    ((TreeNode<K, V>) node).removeTreeNode(this, tab, true);
                } else if (node == p) {
                    tab[index] = node.next;
                } else {
                    p.next = node.next;
                }
                size.decrementAndGet();
                return oldValue;
            }
        }
        return null;
    }

    @Override
    public V get(Object key) {
        Node<K, V> e;
        return (e = this.getNode(key)) == null ? null : e.getValue();
    }

    final Node<K, V> getNode(Object key) {
        Node<K, V>[] tab;
        Node<K, V> first, e;
        int n;
        long hash = hash(key);
        if ((tab = table) != null && (n = tab.length) > 0 &&
                (first = tab[(n - 1) & (int) hash]) != null) {
            if (first.hash == hash) {
                K k;
                if ((k = first.getKey()) == key || (key != null && key.equals(k))) {
                    return first;
                }
            }
            if ((e = first.next) != null) {
                if (first instanceof TreeNode) {
                    return ((TreeNode<K, V>) first).getTreeNode(hash, key);
                }
                do {
                    K k;
                    if (e.hash == hash && ((k = e.getKey()) == key || (key != null && key.equals(k)))) {
                        return e;

                    }
                } while ((e = e.next) != null);
            }
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        return putValue(hash(key), key, value);
    }

    final V putValue(long hash, K key, V value) {
        Node<K, V>[] tab;
        Node<K, V> p;
        int n, i;
        if ((tab = table) == null || (n = tab.length) == 0) {
            n = (tab = resize()).length;
        }
        if ((p = tab[i = (n - 1) & (int) hash]) == null) {
            tab[i] = newNode(hash, key, value, null);
        }
        else {
            Node<K, V> e;
            K k;
            synchronized (p) {
                if (p.hash == hash &&
                        ((k = p.getKey()) == key || (key != null && key.equals(k)))) {
                    e = p;
                } else if (p instanceof TreeNode) {
                    e = ((TreeNode<K, V>) p).putTreeVal(this, tab, hash, key, value);
                }
                else {
                    int binCount = 0;
                    for (e = p; ; ++binCount) {
                        if (e.next == null) {
                            e.next = newNode(hash, key, value, null);
                            if (binCount >= TREEIFY_THRESHOLD - 1) {
                                treeifyBin(tab, hash);
                            }
                            break;
                        }
                        if (e.hash == hash &&
                                ((k = e.getKey()) == key || (key != null && key.equals(k)))) {
                            break;
                        }
                        e = e.next;
                    }
                }
            }
            if (e != null) {
                V oldValue = e.getValue();
                e.setValue(value);
                return oldValue;
            }
        }
        if (size.incrementAndGet() > threshold) {
            resize();
        }
        return null;
    }

    /**
     * 将链表转换为红黑树
     */
    final void treeifyBin(Node<K, V>[] tab, long hash) {
        int n, index;
        Node<K, V> e;
        if (tab == null || (n = tab.length) < MIN_TREEIFY_CAPACITY) {
            resize();
        } else if ((e = tab[index = (n - 1) & (int) hash]) != null) {
            TreeNode<K, V> hd = null, tl = null;
            do {
                TreeNode<K, V> p = replacementTreeNode(e, null);
                if (tl == null) {
                    hd = p;
                } else {
                    p.prev = tl;
                    tl.next = p;
                }
                tl = p;
            } while ((e = e.next) != null);
            if ((tab[index] = hd) != null) {
                hd.treeify(tab);
            }
        }
    }

    TreeNode<K, V> replacementTreeNode(Node<K, V> p, Node<K, V> next) {
        return new TreeNode<>(p.hash, p.getKey(), p.getValue(), next, null);
    }

    Node<K, V> replacementNode(Node<K, V> p, Node<K, V> next) {
        return new Node<>(p.hash, p.getKey(), p.getValue(), next);
    }

    TreeNode<K, V> newTreeNode(long hash, K key, V value, Node<K, V> next, TreeNode<K, V> parent) {
        return new TreeNode<>(hash, key, value, next, parent);
    }

    private Node<K, V> newNode(long hash, K key, V value, Node<K, V> next) {
        return new Node<>(hash, key, value, next);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    @Override
    public void clear() {
        Node<K, V>[] tab;
        if ((tab = table) != null && size.get() > 0) {
            size.set(0);
            for (int i = 0; i < tab.length; ++i) {
                tab[i] = null;
            }
        }
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }
}
