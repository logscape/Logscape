package com.logscape.disco.kv;

import gnu.trove.set.hash.TCharHashSet;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

public class KVRule {
    private final RulesKeyValueExtractor rulesKeyValueExtractor;

    public KVRule(RulesKeyValueExtractor rulesKeyValueExtractor) {
        this.rulesKeyValueExtractor = rulesKeyValueExtractor;
    }

    static class Rule {
        static char smallFrom = 'a';
        static char smallTo = 'z';
        static char bigFrom = 'A';
        static char bigTo = 'Z';
        static char numFrom = '0';
        static char numTo = '9';
        static char dot = '.';
        static Rule alpha = new Rule.Alpha();
        static Rule alphaNumeric = new Rule.AlphaNumeric();
        static Rule alphaNumericDot = new Rule.AlphaNumericDot();
        static Rule alphaNumericColonDot = new Rule.AlphaNumericColonDot();
        static Rule numeric = new Rule.Numeric();
        static Rule none = new Rule.None();
        static Rule notQuote = new Rule.NotChar("\"".charAt(0));
        static Rule notQuote2 = new Rule.NotChar("\'".charAt(0));
        static Rule notSpace = new Rule.NotChar(" ".charAt(0));
        static Rule notXML = new Rule.NotChars('<','>');
        static Map<String,Rule> collectedRules = new ConcurrentHashMap<String, Rule>();

        public static Rule get(String type) {
            if (type == null) return none;
            if (type.contains("(A1)")) return alphaNumeric;
            if (type.contains("(A1.)")) return alphaNumericDot;
            if (type.contains("(A1:.)")) return alphaNumericColonDot;
            else if (type.contains("(Aa)")) return alpha;
            else if (type.contains("(1)")) return numeric;
            else if (type.equalsIgnoreCase("(^\")")) return notQuote;
            else if (type.equalsIgnoreCase("(^ )")) return notSpace;
            else if (type.equalsIgnoreCase("(^<>)")) return notXML;
            else if (type.equalsIgnoreCase("(^')")) return notQuote2;
            else if (type.equalsIgnoreCase("(^, )")) return new NotCharsThree(' ',',','=');
            else if (type.startsWith("(^")) {
                Rule rule = collectedRules.get(type);
                if (rule != null) return rule;
                rule = new NotCharsArray(type.substring(2,type.length()).toCharArray());
                collectedRules.put(type, rule);
                return rule;
            }
            else if (type.startsWith("[")) {
                Rule rule = collectedRules.get(type);
                if (rule != null) return rule;
                rule = new CharsArray(type.substring(1,type.length()).toCharArray());
                collectedRules.put(type, rule);
                return rule;
            }

            else return none;
        }

        public Rule() {
        }

        public boolean isValid(char c) {
            return true;
        }

        public static class None extends Rule {
            public boolean isValid(char c) {
                return true;
            }
        }

        public static class NotChar extends Rule {
            private char cc;

            public NotChar(char cc) {
                this.cc = cc;
            }

            public boolean isValid(char c) {
                return c != cc;
            }
            public String toString() {
                return super.getClass().toString()  + ":" + cc;
            }
        }
        public static class NotChars extends Rule {
            private char cc1;
            private char cc2;

            public NotChars(char cc1, char cc2) {
                this.cc1 = cc1;
                this.cc2 = cc2;
            }

            public boolean isValid(char c) {
                return c != cc1 && c != cc2;
            }
            public String toString() {
                return super.getClass().toString()  + ":[" + cc1 + "][" + cc2 + "]";
            }

        }
        public static class NotCharsThree extends Rule {
            private char cc1;
            private char cc2;
            private char cc3;

            public NotCharsThree(char cc1, char cc2, char cc3) {
                this.cc1 = cc1;
                this.cc2 = cc2;
                this.cc3 = cc3;
            }

            public boolean isValid(char c) {
                return c != cc1 && c != cc2 && c != cc3;
            }
            public String toString() {
                return super.getClass().toString()  + ":[" + cc1 + "][" + cc2 + "],[" + cc3 + "]";
            }

        }
        public static class NotCharsArray extends Rule {

            private final TCharHashSet cset;

            public NotCharsArray(char[] cc) {
                this.cset = new TCharHashSet(cc);
            }

            public boolean isValid(char c) {
                return ! cset.contains(c);
            }
            public String toString() {
                return super.getClass().toString()  + ":[" + "]";
            }

        }
        public static class CharsArray extends NotCharsArray {

            public CharsArray(char[] cc) {
                super(cc);
            }

            public boolean isValid(char c) {
                return !super.isValid(c);
            }

        }

        public static class Alpha extends Rule {
            public boolean isValid(char c) {
                return (c >= smallFrom && c <= smallTo ||
                        c >= bigFrom && c <= bigTo);
            }
        }

        public static class Numeric extends Rule {
            public boolean isValid(char c) {
                return (c >= numFrom && c <= numTo ||
                        c == dot);
            }
        }

        public static class AlphaNumeric extends Rule {
            public boolean isValid(char c) {
                return (c >= smallFrom && c <= smallTo ||
                        c >= bigFrom && c <= bigTo ||
                        c >= numFrom && c <= numTo);
            }
        }

        public static class AlphaNumericDot extends Rule {
            public boolean isValid(char c) {
                return (c >= smallFrom && c <= smallTo ||
                        c >= bigFrom && c <= bigTo ||
                        c >= numFrom && c <= numTo ||
                        c == '.' || c == '_' || c == '-'
                );
            }
        }
        public static class AlphaNumericColonDot extends Rule {
            public boolean isValid(char c) {
                return (c >= smallFrom && c <= smallTo ||
                        c >= bigFrom && c <= bigTo ||
                        c >= numFrom && c <= numTo ||
                        c == '.' || c == '_' || c == '-' ||c == ':'
                );
            }
        }


    }
}