package com.liquidlabs.replicator.data;

import com.liquidlabs.common.UID;
import com.liquidlabs.orm.Id;

public class Meta {

    @Id
    private String id;
    private String hash;
    private String fileName;
    private String pieceInfo;
    private String hostname;
    private Integer port;
    private String path;
    private boolean manager;

    public Meta() {}

    public Meta(String fileName, String path, String hash, String hostname, int port, boolean manager) {
        this.fileName = fileName;
        this.path = path.replace("\\","/");
        this.hash = hash;
        this.hostname = hostname;
        this.port = port;
        this.manager = manager;
        id = UID.getUUIDWithHostNameAndTime() + "-" + fileName;
    }

    public void addPieceInfo(int pieceNumber, int length, String pieceHash, int start) {
        StringBuilder builder = new StringBuilder();
        if (pieceInfo != null) {
            builder.append(pieceInfo).append(",");
        }
        builder.append(pieceNumber).append("~").append(pieceHash).append("~").append(length).append("~").append(start);
        pieceInfo = builder.toString();
    }

    public String getHash() {
        return hash;
    }
    public String getPath() {
        return path;
    }

    public String getFileName() {
        return fileName;
    }

    public String getPieceInfo() {
        return pieceInfo;
    }

    public String getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }

    public PieceInfo [] getPieces() {
        if (pieceInfo == null) {
            return new PieceInfo[0];
        }
        String [] pieceString = pieceInfo.split(",");
        PieceInfo [] pieces = new PieceInfo[pieceString.length];
        for(int i = 0; i < pieces.length; i++) {
            String [] parts = pieceString[i].split("~");
            pieces [i] = new PieceInfo(Integer.valueOf(parts[0]), parts[1], Integer.valueOf(parts[3]), Integer.valueOf(parts[2]));
        }
        return pieces;
    }

    public String getAddress() {
        return hostname + ":" + port;
    }
    public String toString() {
        return getClass().getName() + " id:" + id + " hash:" + hash + " host:" + hostname + ":" + port + " file:" + fileName + " path:" + path;
    }

    public boolean isManager() {
        return manager;
    }

}
