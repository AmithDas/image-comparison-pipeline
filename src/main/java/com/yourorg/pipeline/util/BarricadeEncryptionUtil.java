package com.yourorg.pipeline.util;

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Envelope encryption utility backed by Google Cloud Firestore and Cloud KMS.
 *
 * <h3>Key hierarchy</h3>
 * <pre>
 *   Firestore collection
 *     └── document(key_id)
 *           └── wrapped_dek: base64(KMS-encrypted AES-256 DEK)
 *
 *   Cloud KMS CryptoKey
 *     └── unwrap(wrapped_dek) → raw DEK bytes
 *
 *   AES-256-GCM(DEK)
 *     └── encrypt / decrypt individual payload field values
 * </pre>
 *
 * <h3>Ciphertext wire format</h3>
 * {@code base64( [12-byte IV] [AES-GCM ciphertext + 16-byte auth tag] )}
 *
 * <h3>Initialisation (per Dataflow worker)</h3>
 * Call {@link #configure(String, String)} once before the first encrypt/decrypt,
 * typically from a Beam DoFn's {@code @Setup} method:
 * <pre>
 *   &#64;Setup
 *   public void setup() {
 *       BarricadeEncryptionUtil.configure(firestoreCollection, kmsKeyPath);
 *   }
 * </pre>
 *
 * DEKs are unwrapped lazily on first use and cached in memory for the lifetime
 * of the worker JVM, so each key_id incurs at most one Firestore read + one KMS call.
 */
public final class BarricadeEncryptionUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BarricadeEncryptionUtil.class);

    private static final String ALGORITHM       = "AES/GCM/NoPadding";
    private static final int    IV_BYTES        = 12;   // 96-bit nonce for GCM
    private static final int    TAG_BITS        = 128;  // GCM authentication tag
    private static final String FIRESTORE_FIELD = "wrapped_dek";

    // ── Worker-scoped configuration (set via configure()) ─────────────────────

    private static volatile String firestoreCollection;
    private static volatile String kmsKeyPath;

    // ── Lazily-initialised GCP clients ────────────────────────────────────────

    private static volatile Firestore                  firestoreClient;
    private static volatile KeyManagementServiceClient kmsClient;

    // ── DEK cache: key_id → unwrapped AES key bytes ──────────────────────────

    private static final Map<String, byte[]> DEK_CACHE = new ConcurrentHashMap<>();

    private BarricadeEncryptionUtil() {}

    // ── Configuration ─────────────────────────────────────────────────────────

    /**
     * Configures the Firestore collection and KMS key path used for DEK lookup.
     * Must be called before the first encrypt/decrypt call on each worker JVM.
     *
     * @param collection Firestore collection name (e.g. {@code "dek_store"})
     * @param keyPath    Full KMS CryptoKey resource path
     *                   (e.g. {@code "projects/p/locations/l/keyRings/r/cryptoKeys/k"})
     */
    public static synchronized void configure(String collection, String keyPath) {
        firestoreCollection = collection;
        kmsKeyPath          = keyPath;
        LOG.info("BarricadeEncryptionUtil configured — collection={}, kmsKeyPath={}",
                collection, keyPath);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Decrypts a single field value.
     *
     * @param keyId          Firestore document ID → identifies which DEK to use
     * @param encryptedValue base64-encoded {@code [IV][ciphertext+tag]}
     * @return decrypted plaintext, or {@code null} if {@code encryptedValue} is null
     */
    public static String decrypt(String keyId, String encryptedValue) {
        if (encryptedValue == null) return null;
        try {
            byte[]     dek        = getOrLoadDek(keyId);
            byte[]     cipherData = Base64.getDecoder().decode(encryptedValue);
            ByteBuffer buf        = ByteBuffer.wrap(cipherData);

            byte[] iv = new byte[IV_BYTES];
            buf.get(iv);
            byte[] ciphertext = new byte[buf.remaining()];
            buf.get(ciphertext);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            return new String(cipher.doFinal(ciphertext));

        } catch (Exception e) {
            throw new RuntimeException("Decryption failed for key_id=" + keyId, e);
        }
    }

    /**
     * Encrypts a single field value.
     *
     * @param keyId     Firestore document ID → identifies which DEK to use
     * @param plaintext value to encrypt
     * @return base64-encoded {@code [IV][ciphertext+tag]}, or {@code null} if
     *         {@code plaintext} is null
     */
    public static String encrypt(String keyId, String plaintext) {
        if (plaintext == null) return null;
        try {
            byte[] dek = getOrLoadDek(keyId);
            byte[] iv  = new byte[IV_BYTES];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE,
                    new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes());

            ByteBuffer buf = ByteBuffer.allocate(IV_BYTES + ciphertext.length);
            buf.put(iv);
            buf.put(ciphertext);
            return Base64.getEncoder().encodeToString(buf.array());

        } catch (Exception e) {
            throw new RuntimeException("Encryption failed for key_id=" + keyId, e);
        }
    }

    // ── DEK loading ───────────────────────────────────────────────────────────

    /**
     * Returns the raw DEK bytes for {@code keyId}, loading on first access:
     * <ol>
     *   <li>Reads the {@code wrapped_dek} field from the Firestore document whose ID
     *       equals {@code keyId}.</li>
     *   <li>Calls Cloud KMS {@code decrypt} to unwrap the DEK.</li>
     *   <li>Caches the raw bytes in-process for subsequent calls.</li>
     * </ol>
     */
    private static byte[] getOrLoadDek(String keyId) {
        return DEK_CACHE.computeIfAbsent(keyId, id -> {
            try {
                LOG.info("Loading DEK for key_id={}", id);

                // Step 1 — Firestore: fetch the wrapped DEK document
                DocumentSnapshot doc = getFirestoreClient()
                        .collection(firestoreCollection)
                        .document(id)
                        .get()
                        .get(); // blocking get on the ApiFuture

                if (!doc.exists()) {
                    throw new IllegalArgumentException(
                            "Firestore document not found for key_id=" + id
                            + " in collection=" + firestoreCollection);
                }

                String wrappedDekB64 = doc.getString(FIRESTORE_FIELD);
                if (wrappedDekB64 == null || wrappedDekB64.isBlank()) {
                    throw new IllegalStateException(
                            "Field '" + FIRESTORE_FIELD + "' missing or empty "
                            + "in Firestore document key_id=" + id);
                }

                // Step 2 — KMS: unwrap the DEK
                DecryptResponse response = getKmsClient().decrypt(
                        kmsKeyPath,
                        ByteString.copyFrom(Base64.getDecoder().decode(wrappedDekB64)));

                LOG.debug("DEK successfully unwrapped for key_id={}", id);
                return response.getPlaintext().toByteArray();

            } catch (Exception e) {
                throw new RuntimeException("Failed to load DEK for key_id=" + id, e);
            }
        });
    }

    // ── Lazy GCP client singletons ────────────────────────────────────────────

    private static Firestore getFirestoreClient() {
        if (firestoreClient == null) {
            synchronized (BarricadeEncryptionUtil.class) {
                if (firestoreClient == null) {
                    firestoreClient = FirestoreOptions.getDefaultInstance().getService();
                    LOG.info("Firestore client initialised");
                }
            }
        }
        return firestoreClient;
    }

    private static KeyManagementServiceClient getKmsClient() {
        if (kmsClient == null) {
            synchronized (BarricadeEncryptionUtil.class) {
                if (kmsClient == null) {
                    try {
                        kmsClient = KeyManagementServiceClient.create();
                        LOG.info("KMS client initialised");
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create KMS client", e);
                    }
                }
            }
        }
        return kmsClient;
    }
}
