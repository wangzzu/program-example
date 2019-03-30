package com.matt.test.leetcode.string;

/**
 * leetcode(796 Rotate String): https://leetcode.com/problems/rotate-string/
 *
 * @author matt
 * @date 2019-03-24 22:09
 */
public class RotateString {

    public static void main(String[] args) {
        assert rotateString("abcde", "cdeab");
        assert !rotateString("abcde", "abced");
        System.out.println(rotateString2(null, null));
        System.out.println(rotateString2("", ""));

        String s = "'b\"a";
        String startQuote = "'";
        String endQuote = "'";

        System.out.println(s.startsWith(startQuote));
        System.out.println(s.endsWith(endQuote));
        System.out.println(s.substring(1, s.length()-1));

        assert s.startsWith(startQuote) && s.endsWith(endQuote) : s;
    }

    public static boolean rotateString(String A, String B) {
        if ((A == null && B == null) || (A.length() == 0 && B.length() == 0)) {
            return true;
        }
        boolean ans = false;
        String rotateA = A;
        for (int i = 0; i < A.length(); i++) {
            char c = A.charAt(i);
            rotateA = rotateA.substring(1) + c;
            if (rotateA.equals(B)) {
                ans = true;
            }
        }
        return ans;
    }

    /**
     * this solution just one code，very well
     *
     * @param A
     * @param B
     * @return
     */
    public static boolean rotateString2(String A, String B) {
        return (A == null && B == null) || (A.length() == B.length() && (A + A).contains(B));
    }
}
