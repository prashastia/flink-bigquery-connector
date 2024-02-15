package com.google.cloud.flink.bigquery.common.config;

import com.google.auto.value.AutoValue;

public abstract class CredentialsOptions extends CredentialsOptionsBase {

    /**
     * Creates a builder class for the {@link CredentialsOptions} class.
     *
     * @return A builder class.
     */
    public static CredentialsOptions.Builder builder() {
        return new AutoValue_CredentialsOptions.Builder();
    }

    /** A builder class for the {@link CredentialsOptions} class. */
    @AutoValue.Builder
    public abstract static class Builder {

        /**
         * Sets the credentials using a file system location.
         *
         * @param credentialsFile the path of the credentials file.
         * @return this builder's instance
         */
        public abstract Builder setCredentialsFile(String credentialsFile);

        /**
         * Sets the credentials using a credentials key, encoded in Base64.
         *
         * @param credentialsKey The credentials key.
         * @return this builder's instance
         */
        public abstract Builder setCredentialsKey(String credentialsKey);

        /**
         * Sets the credentials using a GCP access token.
         *
         * @param credentialsToken The GCP access token.
         * @return this builder's instance
         */
        public abstract Builder setAccessToken(String credentialsToken);

        /**
         * Builds a fully initialized {@link CredentialsOptions} instance.
         *
         * @return The {@link CredentialsOptions} instance.
         */
        public abstract CredentialsOptions build();
    }
}
