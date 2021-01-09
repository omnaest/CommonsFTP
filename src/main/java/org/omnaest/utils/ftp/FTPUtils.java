package org.omnaest.utils.ftp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
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

    }

    public static interface FTPResource
    {
        public byte[] asByteArray();

        public InputStream asInputStream();
    }

    public static enum FileType
    {
        ASCII_TEXT, BINARY, AUTO
    }

    public static FTPRequest load()
    {
        return new FTPRequest()
        {
            private String   userName = "";
            private String   password = "";
            private FileType fileType = FileType.AUTO;

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
                    if (FileType.AUTO.equals(this.fileType))
                    {
                        if (StringUtils.endsWith(path, ".txt"))
                        {
                            ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
                        }
                        else
                        {
                            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                        }
                    }
                    else if (FileType.ASCII_TEXT.equals(this.fileType))
                    {
                        ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
                    }
                    else if (FileType.BINARY.equals(this.fileType))
                    {
                        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                    }

                    //
                    resultOk &= ftpClient.login(this.userName, this.password);
                    LOG.info(ftpClient.getReplyString());

                    ftpClient.enterLocalPassiveMode();

                    resultOk &= ftpClient.retrieveFile(path, outputStream);
                    LOG.info(ftpClient.getReplyString());
                    resultOk &= ftpClient.logout();
                    LOG.info(ftpClient.getReplyString());
                    LOG.info("Success:" + resultOk);

                    //
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
                    });
                }
                catch (Exception e)
                {
                    LOG.error("Unexpected error", e);
                    return Optional.empty();
                }
            }
        };
    }
}
