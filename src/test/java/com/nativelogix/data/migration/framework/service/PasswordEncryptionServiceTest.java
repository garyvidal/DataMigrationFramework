package com.nativelogix.data.migration.framework.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import static org.junit.jupiter.api.Assertions.*;

class PasswordEncryptionServiceTest {

    // Single instance reuses the same key (loaded or generated once)
    private final PasswordEncryptionService service = new PasswordEncryptionService();

    // ── Round-trip ────────────────────────────────────────────────────────────

    @Test
    void encryptDecrypt_roundTrip_returnsOriginalValue() {
        String original = "s3cr3tP@ssw0rd!";
        String encrypted = service.encrypt(original);
        assertEquals(original, service.decrypt(encrypted));
    }

    @Test
    void encryptDecrypt_unicodePassword_roundTrip() {
        String original = "pässwörD_日本語_🔑";
        assertEquals(original, service.decrypt(service.encrypt(original)));
    }

    @Test
    void encryptDecrypt_longPassword_roundTrip() {
        String original = "a".repeat(1024);
        assertEquals(original, service.decrypt(service.encrypt(original)));
    }

    // ── ENC: prefix ───────────────────────────────────────────────────────────

    @Test
    void encrypt_producesEncPrefix() {
        String encrypted = service.encrypt("password");
        assertTrue(encrypted.startsWith("ENC:"));
    }

    @Test
    void encrypt_alreadyEncrypted_returnsUnchanged() {
        String encrypted = service.encrypt("password");
        // Calling encrypt again on an already-encrypted value must be idempotent
        assertEquals(encrypted, service.encrypt(encrypted));
    }

    // ── Random IV — same plaintext produces different ciphertexts ─────────────

    @Test
    void encrypt_samePlaintext_producesDifferentCiphertexts() {
        String plain = "same-password";
        String first  = service.encrypt(plain);
        String second = service.encrypt(plain);
        // Random IV means the encoded blobs must differ
        assertNotEquals(first, second);
    }

    // ── Legacy plaintext passthrough ──────────────────────────────────────────

    @Test
    void decrypt_legacyPlaintext_returnedAsIs() {
        // Values without "ENC:" prefix are treated as legacy plaintext
        assertEquals("plaintext-pw", service.decrypt("plaintext-pw"));
    }

    // ── Null / empty passthrough ──────────────────────────────────────────────

    @ParameterizedTest
    @NullAndEmptySource
    void encrypt_nullOrEmpty_returnedUnchanged(String input) {
        assertEquals(input, service.encrypt(input));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void decrypt_nullOrEmpty_returnedUnchanged(String input) {
        assertEquals(input, service.decrypt(input));
    }

    // ── Tampered ciphertext ───────────────────────────────────────────────────

    @Test
    void decrypt_tamperedCiphertext_throwsRuntimeException() {
        String encrypted = service.encrypt("real-password");
        // Corrupt the base64 payload after "ENC:"
        String tampered = "ENC:" + encrypted.substring(4, encrypted.length() - 4) + "XXXX";
        assertThrows(RuntimeException.class, () -> service.decrypt(tampered));
    }
}
