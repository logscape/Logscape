package com.liquidlabs.admin;

import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class PermissionTest {

    @Test
    public void shouldHavePermission() {
        assertTrue(Permission.Read.hasPermission(Permission.Read));
    }

    @Test
    public void shouldHavePermissionWhenMulti() {
        Permission perm = Permission.Read.with(Permission.Write);
        assertTrue(Permission.Read.hasPermission(perm));
        assertTrue(Permission.Write.hasPermission(perm));
    }

    @Test
    public void shouldNotHavePermission() {
        assertFalse(Permission.Configure.hasPermission(Permission.Read));
        assertFalse(Permission.Configure.hasPermission(Permission.Write));
    }

    @Test
    public void shouldHaveAllPermissions() {
        final Permission perms = Permission.Read.with(Permission.Write).with(Permission.Configure);
        assertTrue(perms.hasPermission(Permission.Configure));
        assertTrue(perms.hasPermission(Permission.Read));
        assertTrue(perms.hasPermission(Permission.Write));
    }

    @Test
    public void shouldMakePerm() {
        final Permission perms = Permission.make(7);
        assertTrue(perms.hasPermission(Permission.Configure));
        assertTrue(perms.hasPermission(Permission.Read));
        assertTrue(perms.hasPermission(Permission.Write));
    }

}
