package com.liquidlabs.transport.netty.handshake.common;

public class Challenge {
    public static String clientKey = System.getProperty("client.auth.token","Client");
    public static String serverKey = System.getProperty("server.auth.token","Server");
    private static int result = serverKey.hashCode() + clientKey.hashCode();

    public static String generateChallenge() {
        return clientKey;
    }

    /**
     * Server received from client
     * @param challenge
     * @return
     */
    public static boolean isValidChallenge(String challenge) {
        return result ==  serverKey.hashCode() +  challenge.hashCode();

    }

    /**
     * Server to client
     * @param challenge
     * @return
     */
    public static String generateResponse(String challenge) {
        if (isValidChallenge(challenge)) {
            return "response!";
        } else {
            return "invalidResponse!";
        }
    }

    /**
     * Client Side
     * @param response
     * @param challenge
     * @return
     */
    public static boolean isValidResponse(String response, String challenge) {
        return "response!".equals(response) && result == clientKey .hashCode()+  challenge.hashCode();
    }
}