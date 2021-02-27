package org.omnaest.utils.ftp;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.omnaest.utils.IOUtils;
import org.omnaest.utils.RetryUtils;
import org.omnaest.utils.duration.DurationCapture;
import org.omnaest.utils.duration.DurationCapture.DurationMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FTP related utils
 * 
 * @author omnaest
 */
public class FTPUtils
{
    private static Logger LOG = LoggerFactory.getLogger(FTPUtils.class);

    public static interface FTPRequest
    {
        public FTPRequest withUsername(String userName);

        public FTPRequest withPassword(String password);

        public FTPRequest withAnonymousCredentials();

        public FTPRequest withFileType(FileType fileType);

        public Optional<FTPResource> fromUrl(String url);

        public Optional<FTPResource> from(String host, int port);

        public Optional<FTPResource> from(String host, String path);

        public Optional<FTPResource> from(String host, int port, String path);

        public FTPRequest withReceiveUsingPassiveMode(boolean receiveUsingPassiveMode);

        public FTPRequest withNumberOfRetries(int retryTimes);

    }

    public static interface FTPResource
    {
        public byte[] asByteArray();

        public InputStream asInputStream();

        /**
         * Similar to {@link #asString()} with {@link StandardCharsets#UTF_8}
         * 
         * @return
         */
        public String asString();

        /**
         * Returns the content converted to a {@link String} using the given {@link Charset}
         * 
         * @see StandardCharsets
         * @param charset
         * @return
         */
        public String asString(Charset charset);
    }

    public static enum FileType
    {
        ASCII_TEXT, BINARY, AUTO
    }

    public static FTPRequest load()
    {
        return new FTPRequest()
        {
            private String   userName                = "";
            private String   password                = "";
            private FileType fileType                = FileType.AUTO;
            private boolean  receiveUsingPassiveMode = true;
            private int      retryTimes              = 2;

            @Override
            public FTPRequest withReceiveUsingPassiveMode(boolean receiveUsingPassiveMode)
            {
                this.receiveUsingPassiveMode = receiveUsingPassiveMode;
                return this;
            }

            @Override
            public FTPRequest withNumberOfRetries(int retryTimes)
            {
                this.retryTimes = retryTimes;
                return this;
            }

            @Override
            public FTPRequest withUsername(String userName)
            {
                this.userName = userName;
                return this;
            }

            @Override
            public FTPRequest withPassword(String password)
            {
                this.password = password;
                return this;
            }

            @Override
            public FTPRequest withAnonymousCredentials()
            {
                return this.withUsername("anonymous")
                           .withPassword("");
            }

            @Override
            public Optional<FTPResource> fromUrl(String url)
            {
                try
                {
                    URL fullUrl = new URL(url);
                    String host = fullUrl.getHost();
                    int port = fullUrl.getPort();
                    String path = fullUrl.getPath();
                    return this.from(host, port, path);
                }
                catch (MalformedURLException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }

            @Override
            public Optional<FTPResource> from(String host, int port)
            {
                String path = "";
                return this.from(host, port, path);
            }

            @Override
            public Optional<FTPResource> from(String host, String path)
            {
                int port = -1;
                return this.from(host, port, path);
            }

            @Override
            public FTPRequest withFileType(FileType fileType)
            {
                this.fileType = fileType;
                return this;
            }

            @Override
            public Optional<FTPResource> from(String host, int port, String path)
            {
                return RetryUtils.retry()
                                 .times(this.retryTimes)
                                 .withDurationInBetween(10, ChronoUnit.SECONDS)
                                 .withSingleExceptionFilter(IOException.class)
                                 .silentOperation(() ->
                                 {
                                     Optional<byte[]> content = Optional.empty();
                                     try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream())
                                     {
                                         //
                                         FTPClient ftpClient = new FTPClient();

                                         //
                                         boolean resultOk = true;
                                         if (port >= 0)
                                         {
                                             ftpClient.connect(host, port);
                                         }
                                         else
                                         {
                                             ftpClient.connect(host);
                                         }

                                         //
                                         resultOk &= ftpClient.login(this.userName, this.password);
                                         LOG.info(ftpClient.getReplyString());

                                         //
                                         if (FileType.AUTO.equals(this.fileType))
                                         {
                                             if (StringUtils.endsWithAny(path, ".txt", ".json", ".xml"))
                                             {
                                                 resultOk &= ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
                                             }
                                             else
                                             {
                                                 resultOk &= ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                                             }
                                         }
                                         else if (FileType.ASCII_TEXT.equals(this.fileType))
                                         {
                                             resultOk &= ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
                                         }
                                         else if (FileType.BINARY.equals(this.fileType))
                                         {
                                             resultOk &= ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                                         }
                                         LOG.info(ftpClient.getReplyString());

                                         resultOk &= ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);
                                         LOG.info(ftpClient.getReplyString());

                                         if (this.receiveUsingPassiveMode)
                                         {
                                             ftpClient.enterLocalPassiveMode();
                                         }

                                         long fileSize = this.determineFileSize(path, ftpClient);
                                         ftpClient.setBufferSize(this.determineBufferSize(fileSize));
                                         DurationMeasurement durationMeasurement = DurationCapture.newInstance()
                                                                                                  .start();
                                         LOG.info("File size: " + fileSize);
                                         try (InputStream inputStream = new BufferedInputStream(ftpClient.retrieveFileStream(path), 32 * 1024 * 1024))
                                         {
                                             LOG.info(ftpClient.getReplyString());
                                             IOUtils.copyWithProgess(inputStream, outputStream, fileSize, (int) Math.round(fileSize * 0.01),
                                                                     progress -> LOG.info("Progress: " + Math.round(progress * 100) + "% ETA: "
                                                                             + durationMeasurement.stop()
                                                                                                  .toETA(progress)
                                                                                                  .asCanonicalString(TimeUnit.HOURS, TimeUnit.MINUTES,
                                                                                                                     TimeUnit.SECONDS)
                                                                             + "( " + Math.round(fileSize * progress) + " / " + fileSize + " bytes )"));
                                         }
                                         catch (Exception e)
                                         {
                                             LOG.warn("Exception fetching file content: " + path, e);
                                             resultOk = false;
                                         }

                                         resultOk &= ftpClient.logout();
                                         LOG.info(ftpClient.getReplyString());
                                         LOG.info("Success:" + resultOk);

                                         ftpClient.disconnect();

                                         //
                                         outputStream.flush();
                                         content = !resultOk ? Optional.empty() : Optional.ofNullable(outputStream.toByteArray());

                                         return content.map(data -> new FTPResource()
                                         {
                                             @Override
                                             public InputStream asInputStream()
                                             {
                                                 return new ByteArrayInputStream(data);
                                             }

                                             @Override
                                             public byte[] asByteArray()
                                             {
                                                 return data;
                                             }

                                             @Override
                                             public String asString()
                                             {
                                                 return this.asString(StandardCharsets.UTF_8);
                                             }

                                             @Override
                                             public String asString(Charset charset)
                                             {
                                                 try
                                                 {
                                                     return org.apache.commons.io.IOUtils.toString(this.asInputStream(), charset);
                                                 }
                                                 catch (IOException e)
                                                 {
                                                     throw new IllegalStateException("Unable to convert to string", e);
                                                 }
                                             }

                                         });
                                     }
                                     catch (Exception e)
                                     {
                                         LOG.error("Unexpected error", e);
                                         return Optional.empty();
                                     }
                                 });
            }

            private int determineBufferSize(long fileSize)
            {
                return Math.max(1024, Math.min(256 * 1024 * 1024, (int) fileSize));
            }

            private long determineFileSize(String path, FTPClient ftpClient) throws IOException
            {
                return Optional.ofNullable(ftpClient.listFiles(path))
                               .filter(files -> files.length == 1)
                               .map(files -> files[0])
                               .filter(file -> file.isFile())
                               .map(file -> file.getSize())
                               .orElse(-1l);
            }
        };
    }
}
