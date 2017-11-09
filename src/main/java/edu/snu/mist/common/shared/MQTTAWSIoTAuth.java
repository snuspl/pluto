/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.common.shared;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.eclipse.paho.client.mqttv3.*;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class manages AWS IoT MQTT Authentication.
 */
public final class MQTTAWSIoTAuth {

  /**
   * AWS IoT ROOT CA Certificate.
   */
  private static final String AWS_IOT_ROOT_CA = "rootCA.pem";

  /**
   * Basepath of certificate files.
   */
  private static final String CERT_FILE_BASE_PATH = "src/main/resources/cert/";


  private MQTTAWSIoTAuth() { }

  /**
   * The map containing region-certificates information.
   */
  private static final Map<String, SimpleEntry<String, String>> REGION_CERTIFICATES_MAP;

  static {
    REGION_CERTIFICATES_MAP = new HashMap<>();
    REGION_CERTIFICATES_MAP.put("ap-northeast-2",
        new SimpleEntry<>("b0e945ee3a-certificate.pem.crt", "b0e945ee3a-private.pem.key"));
  }

  private static String getRegion(final String brokerURI) {
    final Pattern pattern = Pattern.compile("iot.(.*).amazonaws.com");
    final Matcher matcher = pattern.matcher(brokerURI);

    return matcher.find() ? matcher.group(1) : "";
  }

  public static boolean needAuth(final String brokerURI) {
    return !getRegion(brokerURI).equals("");
  }

  public static void applyAuth(final MqttConnectOptions options, final String brokerURI) {
    final String region = getRegion(brokerURI);
    final SimpleEntry<String, String> certificates = REGION_CERTIFICATES_MAP.get(region);

    try {
      final SSLSocketFactory socketFactory = getSocketFactory(AWS_IOT_ROOT_CA,
          certificates.getKey(), certificates.getValue(), "");
      options.setSocketFactory(socketFactory);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static String getFilePath(final String file) {
    return CERT_FILE_BASE_PATH + file;
  }

  private static SSLSocketFactory getSocketFactory(final String caCrtFile,
                                                   final String crtFile, final String keyFile, final String password)
      throws Exception {
    Security.addProvider(new BouncyCastleProvider());

    // load CA certificate
    X509Certificate caCert = null;

    final FileInputStream fis = new FileInputStream(getFilePath(caCrtFile));
    final CertificateFactory cf = CertificateFactory.getInstance("X.509");
    BufferedInputStream bis = new BufferedInputStream(fis);

    while (bis.available() > 0) {
      caCert = (X509Certificate) cf.generateCertificate(bis);
    }

    // load client certificate
    bis = new BufferedInputStream(new FileInputStream(getFilePath(crtFile)));
    X509Certificate cert = null;
    while (bis.available() > 0) {
      cert = (X509Certificate) cf.generateCertificate(bis);
    }

    // load client private key
    final PEMParser pemParser = new PEMParser(new FileReader(getFilePath(keyFile)));
    final Object object = pemParser.readObject();
    final PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder()
        .build(password.toCharArray());
    final JcaPEMKeyConverter converter = new JcaPEMKeyConverter()
        .setProvider("BC");
    KeyPair key;
    if (object instanceof PEMEncryptedKeyPair) {
      key = converter.getKeyPair(((PEMEncryptedKeyPair) object)
          .decryptKeyPair(decProv));
    } else {
      key = converter.getKeyPair((PEMKeyPair) object);
    }
    pemParser.close();

    // CA certificate is used to authenticate server
    final KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
    caKs.load(null, null);
    caKs.setCertificateEntry("ca-certificate", caCert);
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
    tmf.init(caKs);

    // client key and certificates are sent to server
    final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    ks.setCertificateEntry("certificate", cert);
    ks.setKeyEntry("private-key", key.getPrivate(), password.toCharArray(),
        new java.security.cert.Certificate[]{cert});
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
        .getDefaultAlgorithm());
    kmf.init(ks, password.toCharArray());

    // finally, create SSL socket factory
    SSLContext context = SSLContext.getInstance("TLSv1.2");
    context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    return context.getSocketFactory();
  }
}
