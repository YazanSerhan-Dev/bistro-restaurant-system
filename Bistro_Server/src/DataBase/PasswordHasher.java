package DataBase;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Hashes and verifies passwords with PBKDF2 (HMAC-SHA256) and a random salt.
 * <p>
 * Stored format: {@code pbkdf2$<iterations>$<salt base64>$<hash base64>} (about 83 characters,
 * fits the existing {@code VARCHAR(100)} password columns).
 * <p>
 * Old databases still contain plain-text passwords. {@link #verify(String, String)} accepts them,
 * and {@link #needsUpgrade(String)} tells the DAO to replace them with a hash after a successful login,
 * so existing accounts keep working and are migrated automatically.
 */
public final class PasswordHasher {

    private static final String PREFIX = "pbkdf2";
    private static final int ITERATIONS = 120_000;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 256;
    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordHasher() {}

    /** Returns a salted PBKDF2 hash of the given password. */
    public static String hash(String password) {
        if (password == null) password = "";
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        byte[] hash = pbkdf2(password.toCharArray(), salt, ITERATIONS);
        Base64.Encoder b64 = Base64.getEncoder();
        return PREFIX + "$" + ITERATIONS + "$" + b64.encodeToString(salt) + "$" + b64.encodeToString(hash);
    }

    /**
     * Checks a password against a stored value (hash, or legacy plain text).
     *
     * @param password the password the user typed
     * @param stored   the value from the database
     * @return true if they match
     */
    public static boolean verify(String password, String stored) {
        if (password == null || stored == null) return false;

        if (!isHashed(stored)) {
            // Legacy plain-text row: constant-time compare
            return MessageDigest.isEqual(password.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    stored.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        String[] parts = stored.split("\\$");
        if (parts.length != 4) return false;
        try {
            int iterations = Integer.parseInt(parts[1]);
            byte[] salt = Base64.getDecoder().decode(parts[2]);
            byte[] expected = Base64.getDecoder().decode(parts[3]);
            byte[] actual = pbkdf2(password.toCharArray(), salt, iterations);
            return MessageDigest.isEqual(expected, actual);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** True if the stored value is already a hash produced by this class. */
    public static boolean isHashed(String stored) {
        return stored != null && stored.startsWith(PREFIX + "$");
    }

    /** True if the stored value should be re-saved as a hash (plain text or weaker settings). */
    public static boolean needsUpgrade(String stored) {
        if (!isHashed(stored)) return true;
        String[] parts = stored.split("\\$");
        try {
            return parts.length != 4 || Integer.parseInt(parts[1]) < ITERATIONS;
        } catch (NumberFormatException e) {
            return true;
        }
    }

    private static byte[] pbkdf2(char[] password, byte[] salt, int iterations) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, HASH_BITS);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException("PBKDF2 is not available", e);
        }
    }
}
