package pri.xiongcheng;

import java.io.*;

/**
 * @author xiongcheng
 */
public class CityHashUtils {
    private static final long k0 = 0xc3a5c85c97cb3127L;
    private static final long k1 = 0xb492b66fbe98f273L;
    private static final long k2 = 0x9ae16a3b2f90404fL;
    private static final long k3 = 0xc949d7c7509e6557L;

    /**
     * byte[]转long,小端在前
     * @param b
     * @param i
     * @return
     */
    private static long toLongLE(byte[] b, int i) {
        return (((long)b[i+7] << 56) +
                ((long)(b[i+6] & 255) << 48) +
                ((long)(b[i+5] & 255) << 40) +
                ((long)(b[i+4] & 255) << 32) +
                ((long)(b[i+3] & 255) << 24) +
                ((b[i+2] & 255) << 16) +
                ((b[i+1] & 255) <<  8) +
                ((b[i+0] & 255) <<  0));
    }
    private static long toIntLE(byte[] b, int i) {
        return (((b[i+3] & 255L) << 24) + ((b[i+2] & 255L) << 16) + ((b[i+1] & 255L) << 8) + ((b[i+0] & 255L) << 0));
    }

    private static long fetch64(byte[] s, int pos) {
        return toLongLE(s, pos);
    }

    private static long fetch32(byte[] s, int pos) {
        return toIntLE(s, pos);
    }

    private static int staticCastToInt(byte b) {
        return b & 0xFF;
    }

    private static long rotate(long val, int shift) {
        // Avoid shifting by 64: doing so yields an undefined result.
        return shift == 0 ? val : (val >>> shift) | (val << (64 - shift));
    }

    // Equivalent to Rotate(), but requires the second arg to be non-zero.
    // On x86-64, and probably others, it's possible for this to compile
    // to a single instruction if both args are already in registers.

    private static long rotateByAtLeast1(long val, int shift) {
        return (val >>> shift) | (val << (64 - shift));
    }

    private static long shiftMix(long val) {
        return val ^ (val >>> 47);
    }

    private static final long kMul = 0x9ddfea08eb382d69L;
    private static long hash128to64(long u, long v) {
        long a = (u ^ v) * kMul;
        a ^= (a >>> 47);
//        long b = (u ^ a) * kMul;
        long b = (v ^ a) * kMul;
        b ^= (b >>> 47);
        b *= kMul;
        return b;
    }

    private static long hashLen16(long u, long v) {
        return hash128to64(u, v);
    }


    private static long hashLen0to16(byte[] s, int pos, int len) {
        if (len > 8) {
            long a = fetch64(s,pos );
            long b = fetch64(s, pos + len - 8);

            return hashLen16(a,rotateByAtLeast1(b + len,len)) ^ b;
        }

        if (len >= 4) {
            long a = fetch32(s, pos );
            return hashLen16(len + (a << 3) , fetch32(s, pos + len - 4));
        }

        if (len > 0) {
            byte a = s[pos];
            byte b = s[pos + (len >>> 1)];
            byte c = s[pos + len - 1];
            int y = staticCastToInt(a) + (staticCastToInt(b) << 8);
            int z = len + (staticCastToInt(c) << 2);
            return shiftMix(y * k2 ^ z * k3) * k2;
        }

        return k2;



    }

    // This probably works well for 16-byte strings as well, but it may be overkill
    // in that case.
    private static long hashLen17to32(byte[] s, int pos, int len){
        long a = fetch64(s, pos) * k1;
        long b = fetch64(s, pos+8);
        long c = fetch64(s, pos+len - 8) * k2;
        long d = fetch64(s,  pos+len - 16) * k0;
        return hashLen16(rotate(a - b, 43) + rotate(c, 30) + d,
                a + rotate(b ^ k3, 20) - c + len);

    }




    private static long hashLen33to64(byte[] s, int pos, int len) {
        long z = fetch64(s,pos + 24);
        long a = fetch64(s, pos) + (len + fetch64(s, pos + len - 16)) * k0;

        long b = rotate(a + z,52);
        long c = rotate(a,37);
        a += fetch64(s,pos + 8);
        c += rotate(a,7);
        a += fetch64(s,pos + 16);
        long vf = a + z;
        long vs = b + rotate(a, 31) + c;
        a = fetch64(s,pos + 16) + fetch64(s , len - 32);
        z = fetch64(s , len - 8);
        b = rotate(a + z, 52);
        c = rotate(a, 37);
        a += fetch64(s , len - 24);
        c += rotate(a, 7);
        a += fetch64(s , len - 16);
        long wf = a + z;
        long ws = b + rotate(a, 31) + c;
        long r = shiftMix((vf + ws) * k2 + (wf + vs) * k0);
        return shiftMix(r * k0 + vs) * k2;
    }


    /**
     * cityHash64
     * @param s
     * @param pos
     * @param len
     * @return
     */
    public static long cityHash64(byte[] s, int pos, int len) {
        if (len <= 32) {
            if (len <= 16) {
                return hashLen0to16(s, pos, len);
            } else {
                return hashLen17to32(s, pos, len);
            }
        } else if (len <= 64) {
            return hashLen33to64(s, pos, len);
        }

        // For strings over 64 bytes we hash the end first, and then as we
        // loop we keep 56 bytes of state: v, w, x, y, and z.
        long x = fetch64(s,pos);
        long y = fetch64(s ,pos + len - 16) ^ k1;
        long z = fetch64(s,pos + len - 56) ^ k0;
        long [] v = weakHashLen32WithSeeds(s, pos + len - 64, len, y);
        long [] w = weakHashLen32WithSeeds(s, pos + len - 32, len * k1, k0);
        z += shiftMix(v[1]) * k1;
        x = rotate(z + x, 39) * k1;
        y = rotate(y, 33) * k1;


        // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
        len = (len - 1) & ~staticCastToInt((byte)63);;
        do {
            x = rotate(x + y + v[0] + fetch64(s,pos + 16), 37) * k1;
            y = rotate(y + v[1] + fetch64(s,pos + 48), 42) * k1;
            x ^= w[1];
            y ^= v[0];
            z = rotate(z ^ w[0], 33);
            v = weakHashLen32WithSeeds(s,pos, v[1] * k1, x + w[0]);
            w = weakHashLen32WithSeeds(s,pos + 32, z + w[1], y);
            long tmp = x;
            x = z;
            z = tmp;
            pos += 64;
            len -= 64;
        } while (len != 0);


        return hashLen16(hashLen16(v[0], w[0]) + shiftMix(y) * k1 + z,
                hashLen16(v[1], w[1]) + x);
    }//cityHash64

    public static long cityHash64(byte[] s){
        int len = s.length;
        int pos = 0;

        return cityHash64(s,pos,len);
    }


    private static long[] weakHashLen32WithSeeds(
            long w, long x, long y, long z,
            long a, long b) {

        a += w;
        b = rotate(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += rotate(a, 44);
        return new long[]{ a + z, b + c };

    }



    // Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
    private static long[] weakHashLen32WithSeeds(byte[] s, int pos, long a, long b) {
        return weakHashLen32WithSeeds(
                fetch64(s, pos + 0),
                fetch64(s, pos + 8),
                fetch64(s, pos + 16),
                fetch64(s, pos + 24),
                a,
                b
        );
    }

    public static byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    public static Object toObject(byte[] bytes) {
        Object obj = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            obj = objectInputStream.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (byteArrayInputStream != null) {
                try {
                    byteArrayInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return obj;
    }
}
