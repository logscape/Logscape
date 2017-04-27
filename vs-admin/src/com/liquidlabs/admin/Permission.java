package com.liquidlabs.admin;

public class Permission {


    public static final Permission None = new Permission("None", 0);
    public static final Permission Read = new Permission("Read", 0x1);
    public static final Permission Write = new Permission("Write", 0x2);
    public static final Permission Configure  = new Permission("Configure",0x4);
    public static final Permission All  = new Permission("All", 7);

    private final int perm;
    private final String name;


    public Permission() {
        this("None", 0); // for serialization
    }

    private Permission(String name, int perm) {
        this.perm = perm;
        this.name = name;
    }

    public boolean hasPermission(Permission value) {
        return permissionInternal(value.perm);
    }



    public Permission with(Permission permission) {
        return new Permission(name + "|" + permission.name, perm | permission.perm);
    }
    public static Permission make(User.ROLE role) {
        if (role.equals(User.ROLE.Read_Only_User)) return Permission.Read;
        if (role.equals(User.ROLE.Read_Write_User)) return Permission.Read.with(Permission.Write);
        if (role.equals(User.ROLE.Team_Administrator)) return Permission.Read.with(Permission.All);
        if (role.equals(User.ROLE.System_Administrator)) return Permission.Read.with(Permission.All);
        return Permission.Read;
    }

    public static Permission make(int perm) {
        Permission mine = None;
        if(Read.permissionInternal(perm)) {
           mine = Read;
        }
        if(Write.permissionInternal(perm)) {
            if(mine == None) mine = Write;
            else mine = mine.with(Write);
        }
        if(Configure.permissionInternal(perm)){
            if(mine == None) mine = Configure;
            else mine = mine.with(Configure);
        }
        return mine;
    }

    private boolean permissionInternal(int other) {
        final int i = perm & other;
        return i != 0;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public int getPerm() {
        return perm;
    }
}
