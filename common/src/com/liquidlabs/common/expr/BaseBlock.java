package com.liquidlabs.common.expr;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 10/04/13
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
abstract class BaseBlock implements Block {
    private final String token;
    private final String givenToken;
    protected final String remainder;


    protected BaseBlock(String token, String givenToken) {

        this.token = token;
        this.givenToken = givenToken;
        this.remainder = givenToken.replace(token,"");
    }

    abstract boolean isGood(char c);
    public boolean isFailed(String data, int endPos, int collectedlength) {
        if (collectedlength == 0) return true;
        if (data.charAt(endPos) != remainder.charAt(0)) return true;
        return false;
    }

    int readAmount;
    public int skip(){
        return readAmount +1;
    }
    public String get(int pos, String string) {
        boolean good = true;
        int cpos = pos;
        while (good) {
            if (isGood(string.charAt(cpos))) {
                cpos++;
            } else {
                good = false;
            }
        }
        // match failed
        if (cpos == 0) return null;
        readAmount = cpos - pos;
        if (isFailed(string, cpos, readAmount)) return null;
        return string.substring(pos, cpos);
    }

    char smallFrom = 'a';
    char smallTo = 'z';
    char bigFrom = 'A';
    char bigTo = 'Z';
    char numFrom = '0';
    char numTo = '9';
    char dot = '.';

}
