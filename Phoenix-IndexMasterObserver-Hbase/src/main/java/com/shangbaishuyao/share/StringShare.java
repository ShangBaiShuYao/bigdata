package com.shangbaishuyao.share;

/**
 * @author lan
 * @create 2019-11-15-21:50
 */
public class StringShare {
    public static void main(String[] args) {

//        String[] arr = {"AABBCCDD", "TT", "RR"}; // 000
//        String[] arr = {"AABBCCDD", "AA", "RR"}; // 100 // result2Error
//        String[] arr = {"AABBCCDD", "BB", "RR"}; // 010
//        String[] arr = {"AABBCCDD", "DD", "RR"}; // 001
//        String[] arr = {"AABBAADD", "AA", "RR"}; // 110 // result2Error
//        String[] arr = {"AABBCCAA", "AA", "RR"}; // 101
//        String[] arr = {"AABBCCBB", "BB", "RR"}; // 011 // result2Error
        String[] arr = {"AABBAAAA", "AA", "RR"}; // 111 // result2Error
//        String[] arr = {"aabbccbb", "bb", "dd"};
        final String text = arr[0];
        final String target = arr[1];
        final String replace = arr[2];

        String result1 = replace1(text, target, replace);
//        String result2 = replace2(text, target, replace);

        System.out.println(
                "Ä¿±ê×Ö·û´®text = " + text + "\n" +
                        "±»Ìæ»»×Ö·û´®target = " + target + "\n" +
                        "Ìæ»»×Ö·û´®replace = " + replace + "\n");

        System.out.println("Ìæ»»ºóÄ¿±ê×Ö·û´®Îª" + "\n" +
                "result1 = " + result1);
//                + "\n" +
//                "result2 = " + result2);
    }

    private static String replace1(String text, String target, String replace) {

        StringBuilder sb = new StringBuilder();
        int index = text.indexOf(target);

        if (index == -1) {
            return text;
        }
        while (index != -1) {
            sb.append(text.substring(0, index)).append(replace);
            text = text.substring(index + target.length());
            index = text.indexOf(target);
        }
        return sb.append(text).toString();
    }

    private static String replace2(String text, String target, String replace) {

        String[] s = text.split(target);
        StringBuilder sb = new StringBuilder();
        int index = text.indexOf(target);

        boolean a = false;
        boolean b = false;

        if (index == -1) {
            return text;
        }

        while (index != -1) {
            if (index == 0) {
                a = true;
            }
            if (index == (text.length() - target.length())) {
                b = true;
            }

            if (!a && !b) {// a¡¢b¾ùÎªfalse
                add(s, sb, replace);
            } else if (a && !b) {// aÎªtrue£¬bÎªfalse
                sb.append(replace);
                add(s, sb, replace);
            } else if (!a && b) {// aÎªfalse£¬bÎªtrue
                add(s, sb, replace);
                sb.append(replace);
            } else {// a¡¢b¾ùÎªtrue
                sb.append(replace);
                add(s, sb, replace);
                sb.append(replace);
            }

            text = text.substring(index + target.length());
            index = text.indexOf(target);
        }

//        System.out.println("a = " + a + ", b = " + b); //a = false, b = true
        /*if (!a && !b) {// a¡¢b¾ùÎªfalse
            add(s, sb, replace);
        } else if (a && !b) {// aÎªtrue£¬bÎªfalse
            sb.append(replace);
            add(s, sb, replace);
        } else if (!a && b) {// aÎªfalse£¬bÎªtrue
            add(s, sb, replace);
            sb.append(replace);
        } else {// a¡¢b¾ùÎªtrue
            sb.append(replace);
            add(s, sb, replace);
            sb.append(replace);
        }*/

        return sb.toString();
    }

    private static void add(String[] s, StringBuilder sb, String replace) {
        for (int i = 0; i < s.length; i++) {
            if (i < s.length - 1) {
                sb.append(s[i]).append(replace);
            } else {
                sb.append(s[i]);
            }
        }
    }


}