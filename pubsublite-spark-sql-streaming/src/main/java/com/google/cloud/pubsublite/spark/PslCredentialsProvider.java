package com.google.cloud.pubsublite.spark;

import com.google.api.client.util.Base64;
import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

public class PslCredentialsProvider implements CredentialsProvider {

  private final GoogleCredentials credentials;

  public PslCredentialsProvider(PslDataSourceOptions options) {
    if (options.credentialsAccessToken() != null) {
      this.credentials = createCredentialsFromAccessToken(options.credentialsAccessToken());
    } else if (options.credentialsFile() != null) {
      this.credentials = createCredentialsFromFile(options.credentialsFile());
    } else {
      this.credentials = createDefaultCredentials();
    }
  }

  private static GoogleCredentials createCredentialsFromAccessToken(String accessToken) {
    return GoogleCredentials.create(new AccessToken(accessToken, null));
  }

  private static GoogleCredentials createCredentialsFromFile(String file) {
    try {
      GoogleCredentials cred = GoogleCredentials.fromStream(new FileInputStream(file))
              .createScoped("https://www.googleapis.com/auth/cloud-platform");
      AccessToken token = cred.refreshAccessToken();
      return GoogleCredentials.create(token);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Credentials from file", e);
    }
  }

  public static GoogleCredentials createDefaultCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create default Credentials", e);
    }
  }

  @Override
  public Credentials getCredentials() {
    return credentials;
  }
}
