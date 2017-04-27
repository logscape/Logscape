package com.liquidlabs.common.expr;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 10/04/13
 * Time: 20:30
 * To change this template use File | Settings | File Templates.
 */
public interface Block {

    Block isMe(String sample);
    int skip();
    String get(int pos, String string);
}
