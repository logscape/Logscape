/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.jboss.netty.util;

/**
 * Overrides the thread name proposed by {@link ThreadRenamingRunnable}.
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @version $Rev: 2080 $, $Date: 2010-01-26 18:04:19 +0900 (Tue, 26 Jan 2010) $
 */
public interface ThreadNameDeterminer {

    public static final String NIO = "NIO";

	public static final String NEW_I_O = "New_I/O";

	public static final String ID = "id:";

	public static final String ID__ = "id:_";

	public static final String P = "p:";

	public static final String PARENT_ID__ = "parentId:_";

	public static final String OI_O_SVR = "OiOSvr";

	public static final String OLD_I_O_SERVER = "Old_I/O_server";

	public static final String USCORE = "_";

	public static final String SPACE = " ";

	public static final String COMMA = ",";

	public static final String COMMA_SPACE = ", ";

	/**
     * {@link ThreadNameDeterminer} that accepts the proposed thread name
     * as is.
     */
    ThreadNameDeterminer PROPOSED = new ThreadNameDeterminer() {
        public String determineThreadName(String currentThreadName,
                String proposedThreadName) throws Exception {
        	return proposedThreadName.replaceAll(COMMA_SPACE,COMMA).replaceAll(SPACE, USCORE).replaceAll(OLD_I_O_SERVER, OI_O_SVR).replace(PARENT_ID__, P).replace(ID__, ID).replace(NEW_I_O, NIO);
//            return proposedThreadName;
        }
    };

    /**
     * {@link ThreadNameDeterminer} that rejects the proposed thread name and
     * retains the current one.
     */
    ThreadNameDeterminer CURRENT = new ThreadNameDeterminer() {
        public String determineThreadName(String currentThreadName,
                String proposedThreadName) throws Exception {
            return null;
        }
    };

    /**
     * Overrides the thread name proposed by {@link ThreadRenamingRunnable}.
     *
     * @param currentThreadName   the current thread name
     * @param proposedThreadName  the proposed new thread name
     * @return the actual new thread name.
     *         If {@code null} is returned, the proposed thread name is
     *         discarded (i.e. no rename).
     */
    String determineThreadName(String currentThreadName, String proposedThreadName) throws Exception;
}
