package de.invesdwin.context.integration.ftp.server.internal;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.AnonymousAuthentication;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.AbstractUserManager;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginRequest;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.TransferRateRequest;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.ftpserver.usermanager.impl.WriteRequest;
import org.apache.ftpserver.util.BaseProperties;

import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.Strings;

/**
 * <table>
 * <tr>
 * <th>Property</th>
 * <th>Documentation</th>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.homedirectory</td>
 * <td>Path to the home directory for the user, based on the file system implementation used</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.userpassword</td>
 * <td>The password for the user. Can be in clear text, MD5 hash or salted SHA hash based on the configuration on the
 * user manager</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.enableflag</td>
 * <td>true if the user is enabled, false otherwise</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.writepermission</td>
 * <td>true if the user is allowed to upload files and create directories, false otherwise</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.idletime</td>
 * <td>The number of seconds the user is allowed to be idle before disconnected. 0 disables the idle timeout</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.maxloginnumber</td>
 * <td>The maximum number of concurrent logins by the user. 0 disables the check.</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.maxloginperip</td>
 * <td>The maximum number of concurrent logins from the same IP address by the user. 0 disables the check.</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.uploadrate</td>
 * <td>The maximum number of bytes per second the user is allowed to upload files. 0 disables the check.</td>
 * </tr>
 * <tr>
 * <td>ftpserver.user.{username}.downloadrate</td>
 * <td>The maximum number of bytes per second the user is allowed to download files. 0 disables the check.</td>
 * </tr>
 * </table>
 * 
 * <p>
 * Example:
 * </p>
 * 
 * <pre>
 * ftpserver.user.admin.homedirectory=/ftproot
 * ftpserver.user.admin.userpassword=admin
 * ftpserver.user.admin.enableflag=true
 * ftpserver.user.admin.writepermission=true
 * ftpserver.user.admin.idletime=0
 * ftpserver.user.admin.maxloginnumber=0
 * ftpserver.user.admin.maxloginperip=0
 * ftpserver.user.admin.uploadrate=0
 * ftpserver.user.admin.downloadrate=0
 * </pre>
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
@NotThreadSafe
public class InMemoryUserManager extends AbstractUserManager {

    private static final String PREFIX = "ftpserver.user.";

    private BaseProperties userDataProp;

    public InMemoryUserManager(final PasswordEncryptor passwordEncryptor, final String adminName) {
        super(adminName, passwordEncryptor);
        userDataProp = new BaseProperties();
    }

    /**
     * Save user data. Store the properties.
     */
    @Override
    public synchronized void save(final User usr) throws FtpException {
        // null value check
        if (usr.getName() == null) {
            throw new NullPointerException("User name is null.");
        }
        final String thisPrefix = PREFIX + usr.getName() + '.';

        // set other properties
        userDataProp.setProperty(thisPrefix + ATTR_PASSWORD, getPassword(usr));

        String home = usr.getHomeDirectory();
        if (home == null) {
            home = "/";
        }
        userDataProp.setProperty(thisPrefix + ATTR_HOME, home);
        userDataProp.setProperty(thisPrefix + ATTR_ENABLE, usr.getEnabled());
        userDataProp.setProperty(thisPrefix + ATTR_WRITE_PERM, usr.authorize(new WriteRequest()) != null);
        userDataProp.setProperty(thisPrefix + ATTR_MAX_IDLE_TIME, usr.getMaxIdleTime());

        TransferRateRequest transferRateRequest = new TransferRateRequest();
        transferRateRequest = (TransferRateRequest) usr.authorize(transferRateRequest);

        if (transferRateRequest != null) {
            userDataProp.setProperty(thisPrefix + ATTR_MAX_UPLOAD_RATE, transferRateRequest.getMaxUploadRate());
            userDataProp.setProperty(thisPrefix + ATTR_MAX_DOWNLOAD_RATE, transferRateRequest.getMaxDownloadRate());
        } else {
            userDataProp.remove(thisPrefix + ATTR_MAX_UPLOAD_RATE);
            userDataProp.remove(thisPrefix + ATTR_MAX_DOWNLOAD_RATE);
        }

        // request that always will succeed
        ConcurrentLoginRequest concurrentLoginRequest = new ConcurrentLoginRequest(0, 0);
        concurrentLoginRequest = (ConcurrentLoginRequest) usr.authorize(concurrentLoginRequest);

        if (concurrentLoginRequest != null) {
            userDataProp.setProperty(thisPrefix + ATTR_MAX_LOGIN_NUMBER,
                    concurrentLoginRequest.getMaxConcurrentLogins());
            userDataProp.setProperty(thisPrefix + ATTR_MAX_LOGIN_PER_IP,
                    concurrentLoginRequest.getMaxConcurrentLoginsPerIP());
        } else {
            userDataProp.remove(thisPrefix + ATTR_MAX_LOGIN_NUMBER);
            userDataProp.remove(thisPrefix + ATTR_MAX_LOGIN_PER_IP);
        }
    }

    /**
     * Delete an user. Removes all this user entries from the properties. After removing the corresponding from the
     * properties, save the data.
     */
    @Override
    public void delete(final String usrName) throws FtpException {
        // remove entries from properties
        final String thisPrefix = PREFIX + usrName + '.';
        final Enumeration<?> propNames = userDataProp.propertyNames();
        final ArrayList<String> remKeys = new ArrayList<String>();
        while (propNames.hasMoreElements()) {
            final String thisKey = propNames.nextElement().toString();
            if (thisKey.startsWith(thisPrefix)) {
                remKeys.add(thisKey);
            }
        }
        final Iterator<String> remKeysIt = remKeys.iterator();
        while (remKeysIt.hasNext()) {
            userDataProp.remove(remKeysIt.next());
        }
    }

    /**
     * Get user password. Returns the encrypted value.
     * 
     * <pre>
     * If the password value is not null
     *    password = new password
     * else
     *   if user does exist
     *     password = old password
     *   else
     *     password = &quot;&quot;
     * </pre>
     */
    private String getPassword(final User usr) {
        final String name = usr.getName();
        String password = usr.getPassword();

        if (password != null) {
            password = getPasswordEncryptor().encrypt(password);
        } else {
            final String blankPassword = getPasswordEncryptor().encrypt("");

            if (doesExist(name)) {
                final String key = PREFIX + name + '.' + ATTR_PASSWORD;
                password = userDataProp.getProperty(key, blankPassword);
            } else {
                password = blankPassword;
            }
        }
        return password;
    }

    /**
     * Get all user names.
     */
    @Override
    public String[] getAllUserNames() {
        // get all user names
        final String suffix = '.' + ATTR_HOME;
        final ArrayList<String> ulst = new ArrayList<String>();
        final Enumeration<?> allKeys = userDataProp.propertyNames();
        final int prefixlen = PREFIX.length();
        final int suffixlen = suffix.length();
        while (allKeys.hasMoreElements()) {
            final String key = (String) allKeys.nextElement();
            if (key.endsWith(suffix)) {
                String name = key.substring(prefixlen);
                final int endIndex = name.length() - suffixlen;
                name = name.substring(0, endIndex);
                ulst.add(name);
            }
        }

        Collections.sort(ulst);
        return ulst.toArray(Strings.EMPTY_ARRAY);
    }

    /**
     * Load user data.
     */
    @Override
    public User getUserByName(final String userName) {
        if (!doesExist(userName)) {
            return null;
        }

        final String baseKey = PREFIX + userName + '.';
        final BaseUser user = new BaseUser();
        user.setName(userName);
        user.setEnabled(userDataProp.getBoolean(baseKey + ATTR_ENABLE, true));
        user.setHomeDirectory(userDataProp.getProperty(baseKey + ATTR_HOME, "/"));

        final List<Authority> authorities = new ArrayList<Authority>();

        if (userDataProp.getBoolean(baseKey + ATTR_WRITE_PERM, false)) {
            authorities.add(new WritePermission());
        }

        final int maxLogin = userDataProp.getInteger(baseKey + ATTR_MAX_LOGIN_NUMBER, 0);
        final int maxLoginPerIP = userDataProp.getInteger(baseKey + ATTR_MAX_LOGIN_PER_IP, 0);

        authorities.add(new ConcurrentLoginPermission(maxLogin, maxLoginPerIP));

        final int uploadRate = userDataProp.getInteger(baseKey + ATTR_MAX_UPLOAD_RATE, 0);
        final int downloadRate = userDataProp.getInteger(baseKey + ATTR_MAX_DOWNLOAD_RATE, 0);

        authorities.add(new TransferRatePermission(downloadRate, uploadRate));

        user.setAuthorities(authorities);

        user.setMaxIdleTime(userDataProp.getInteger(baseKey + ATTR_MAX_IDLE_TIME, 0));

        return user;
    }

    /**
     * User existance check
     */
    @Override
    public boolean doesExist(final String name) {
        final String key = PREFIX + name + '.' + ATTR_HOME;
        return userDataProp.containsKey(key);
    }

    /**
     * User authenticate method
     */
    @Override
    public User authenticate(final Authentication authentication) throws AuthenticationFailedException {
        if (authentication instanceof UsernamePasswordAuthentication) {
            final UsernamePasswordAuthentication upauth = (UsernamePasswordAuthentication) authentication;

            final String user = upauth.getUsername();
            String password = upauth.getPassword();

            if (user == null) {
                throw new AuthenticationFailedException("Authentication failed");
            }

            if (password == null) {
                password = "";
            }

            final String storedPassword = userDataProp.getProperty(PREFIX + user + '.' + ATTR_PASSWORD);

            if (storedPassword == null) {
                // user does not exist
                throw new AuthenticationFailedException("Authentication failed");
            }

            if (getPasswordEncryptor().matches(password, storedPassword)) {
                return getUserByName(user);
            } else {
                throw new AuthenticationFailedException("Authentication failed");
            }

        } else if (authentication instanceof AnonymousAuthentication) {
            if (doesExist("anonymous")) {
                return getUserByName("anonymous");
            } else {
                throw new AuthenticationFailedException("Authentication failed");
            }
        } else {
            throw new IllegalArgumentException("Authentication not supported by this user manager");
        }
    }

    /**
     * Close the user manager - remove existing entries.
     */
    public synchronized void dispose() {
        if (userDataProp != null) {
            userDataProp.clear();
            userDataProp = null;
        }
    }

}
