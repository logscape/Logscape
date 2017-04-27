package com.logscape.disco.kv;

import java.util.ArrayList;
import java.util.List;

public class KeySlider {

    private String kvSig;
    RulesKeyValueExtractor.SliderRule config;

    public List<RulesKeyValueExtractor.Pair> results() {
            return results;
        }

        public void reset() {
            currentState = STATE.BEFORE;
            myPos = 0;
            started = 0;
            ended = 0;
            results.clear();
        }

    public String getRule() {
        return kvSig;
    }

    enum STATE {BEFORE, KEY, SEPERATOR, VALUE}


        private KeySlider.STATE currentState = STATE.BEFORE;
        KVRule.Rule fromTokens;

        private String toToken;
        private KVRule.Rule keyRule;
        private KVRule.Rule valueRule;
        private KVRule.Rule postRule;

        public KeySlider(String kvSig) {
            this.kvSig = kvSig;
            init();
        }
        public KeySlider(RulesKeyValueExtractor.SliderRule sliderRule) {
            this.config = sliderRule;
            this.kvSig = sliderRule.rule;
            init();
        }

    private void init() {
        toToken = RulesKeyValueExtractor.getPostText(this.kvSig);
        keyRule = RulesKeyValueExtractor.getKeyRule(this.kvSig);
        valueRule = RulesKeyValueExtractor.getValueRule(this.kvSig);
        postRule = RulesKeyValueExtractor.getPostRule(this.kvSig);
        fromTokens = RulesKeyValueExtractor.getPreText(this.kvSig);
    }

    int myPos = 0;
        int started = 0;
        int ended = 0;
        List<RulesKeyValueExtractor.Pair> results = new ArrayList<RulesKeyValueExtractor.Pair>();
        boolean bail = true;

        final public boolean next(final char c, final int pos, final int totalLength) {
//            System.out.println(c + " :" + currentState + " started:" + started + " POS:" + pos +  " mpos:" + myPos + " ");
            if (currentState == STATE.BEFORE) {
                if (fromTokens.isValid(c)) {
                    currentState = STATE.KEY;
                    started = pos;
                    myPos = 0;
                }
                //handleBefore(c, pos);
                return false;
            }

            if (currentState == STATE.KEY) {
                //if (handleKey(c, pos)) return false;
                // inline it for performance
                if (!keyRule.isValid(c)) {
                    // bail - invalid char and we haven't collected anything - keys need to be 3 or more chars
                    if (myPos < 3 && !fromTokens.isValid(c)) {
                        revertState();
                        return false;
                    }
                    // did we hit the end
                    if (c == toToken.charAt(0)) {
                        currentState = STATE.SEPERATOR;
                        myPos = 0;
                        // did we have another start token?
                    } else if (fromTokens.isValid(c)) {
                        myPos = 0;
                        currentState = STATE.KEY;
                        started = pos;
                    }
                    // or was it a bad char - start again
                    else currentState = STATE.BEFORE;
                } else {
                    myPos++;
                }
            }

            if (currentState == STATE.SEPERATOR) {
                // only commit the favorite when we find a key followed by a seperator
                return handleSep(c, pos);

            } else if (currentState == STATE.VALUE) {
                boolean isValid = valueRule.isValid(c);
                if (myPos == 0 && !isValid) {
                    myPos = 0;
                    currentState = STATE.BEFORE;
                    return false;
                }

                if (!isValid) {
                    if (postRule != null) {
                        if (postRule.isValid(c)) {
                            return handleValue(c, pos);
                        } else {
                            myPos = 0;
                            currentState = STATE.BEFORE;
                            return false;
                        }
                    }
                    return handleValue(c, pos);
                }


                // hit EOL
                if (pos == totalLength - 1) {
                    // need to add +1 when at the end of line
                    if (pos == totalLength - 1) {
                        return handleValue(c, pos+1);
                    }
                    else  return handleValue(c, pos);

                } else {
                    myPos++;
                }
            }
            return false;
        }

    final private boolean handleValue(final char c, final int pos) {
        // we hit the end when we have collected > 1 character
        //new Pair(started ,ended, pos);

        // dont collect massive values
        if (pos - ended < RulesKeyValueExtractor.maxFieldValueLength && ended- started < RulesKeyValueExtractor.maxFieldValueLength){
            results.add(new RulesKeyValueExtractor.Pair(started, ended, pos, toToken));
        }
        myPos = 0;

        // now figure out if this is a pre-key character or junk
        if (fromTokens.isValid(c)) {
            currentState = STATE.KEY;
            started = pos;
        } else {
            revertState();
        }
        return true;
    }

    final private boolean handleSep(final char c, final int pos) {
        if (myPos < toToken.length() && toToken.charAt(myPos) == c) {
            myPos++;
        } else {
            currentState = STATE.BEFORE;
            if (handleKey(c, pos)) {
                myPos = 0;
                started = pos-1;
                currentState = STATE.KEY;
            }
            return false;
        }
        // NEXT
        if (myPos == toToken.length()) {
            currentState = STATE.VALUE;
            ended = pos;
            myPos = 0;
        }
        return false;
    }

    final private boolean handleKey(final char c, final int pos) {
        if (!keyRule.isValid(c)) {
            // bail - invalid char and we havent collected anything - keys need to be 3 or more chars
            if (myPos < 3 && !fromTokens.isValid(c)) {
                revertState();
                return false;
            }
            // did we hit the end
            if (c == toToken.charAt(0)) {
                currentState = STATE.SEPERATOR;
                myPos = 0;
                // did we have another start token?
            } else if (fromTokens.isValid(c)) {
                myPos = 0;
                currentState = STATE.KEY;
                started = pos;
            }
            // or was it a bad char - start again
            else currentState = STATE.BEFORE;
            return false;
        } else {
            return true;
        }
    }

    final private void handleBefore(char c, int pos) {
        if (fromTokens.isValid(c)) {
            currentState = STATE.KEY;
            started = pos;
            myPos = 0;
        }
    }

    /**
     * inline where possible
     */
    final private void revertState() {
            myPos = 0;
            currentState = STATE.BEFORE;
    }
}