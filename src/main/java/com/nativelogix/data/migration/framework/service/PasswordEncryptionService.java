package com.nativelogix.data.migration.framework.service;

import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM password encryption service.
 * Encryption key is auto-generated on first use and persisted at
 * ~/.rdbms2marklogic/encryption.key (owner-read-only where OS permits).
 *
 * Encrypted values are prefixed with "ENC:" so legacy plaintext passwords
 * in existing JSON files can be detected and migrated transparently.
 */
@Service
public class PasswordEncryptionService {

    public static final String ENC_PREFIX = "ENC:";
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_BITS = 128;
    private static final int KEY_BITS = 256;

    private final SecretKey secretKey;

    public PasswordEncryptionService() {
        try {
            this.secretKey = loadOrGenerateKey();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize password encryption service", e);
        }
    }

    /**
     * Encrypts a plaintext password.
     * Returns null/empty unchanged; otherwise returns "ENC:<base64(iv+ciphertext)>".
     */
    public String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isEmpty()) return plaintext;
        if (plaintext.startsWith(ENC_PREFIX)) return plaintext; // already encrypted
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            byte[] blob = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, blob, 0, iv.length);
            System.arraycopy(ciphertext, 0, blob, iv.length, ciphertext.length);

            return ENC_PREFIX + Base64.getEncoder().encodeToString(blob);
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt password", e);
        }
    }

    /**
     * Decrypts a password value.
     * - "ENC:..." values are decrypted with AES-256-GCM.
     * - Legacy plaintext values (no prefix) are returned as-is for migration.
     * - null/empty are returned unchanged.
     */
    public String decrypt(String value) {
        if (value == null || value.isEmpty()) return value;
        if (!value.startsWith(ENC_PREFIX)) {
            // Legacy plaintext — will be encrypted on next save
            return value;
        }
        try {
            byte[] blob = Base64.getDecoder().decode(value.substring(ENC_PREFIX.length()));
            byte[] iv = new byte[GCM_IV_LENGTH];
            byte[] ciphertext = new byte[blob.length - GCM_IV_LENGTH];
            System.arraycopy(blob, 0, iv, 0, iv.length);
            System.arraycopy(blob, iv.length, ciphertext, 0, ciphertext.length);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt password", e);
        }
    }

    // ── Key management ────────────────────────────────────────────────────────

    private SecretKey loadOrGenerateKey() throws Exception {
        Path keyPath = Paths.get(System.getProperty("user.home"), ".rdbms2marklogic", "encryption.key");
        Files.createDirectories(keyPath.getParent());

        if (Files.exists(keyPath)) {
            byte[] keyBytes = Base64.getDecoder().decode(Files.readString(keyPath).trim());
            return new SecretKeySpec(keyBytes, "AES");
        }

        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(KEY_BITS, new SecureRandom());
        SecretKey key = keyGen.generateKey();
        Files.writeString(keyPath, Base64.getEncoder().encodeToString(key.getEncoded()));
        setOwnerOnlyPermissions(keyPath);
        return key;
    }

    /** Best-effort owner-only file permissions (no-op on Windows if unsupported). */
    private void setOwnerOnlyPermissions(Path path) {
        try {
            path.toFile().setReadable(false, false);
            path.toFile().setReadable(true, true);
            path.toFile().setWritable(false, false);
            path.toFile().setWritable(true, true);
        } catch (Exception ignored) {
        }
    }
}
