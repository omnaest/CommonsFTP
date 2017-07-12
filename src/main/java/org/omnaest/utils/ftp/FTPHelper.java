/*

	Copyright 2017 Danny Kunz

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.


*/
package org.omnaest.utils.ftp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPHelper
{
	private static Logger LOG = LoggerFactory.getLogger(FTPHelper.class);

	public static InputStream loadFileContent(String url) throws IOException
	{
		URL fullUrl = new URL(url);
		String host = fullUrl.getHost();
		String path = fullUrl.getPath();
		int port = fullUrl.getPort();
		return loadFileContent(host, port, path);
	}

	public static InputStream loadFileContent(String host, String path) throws IOException
	{
		int port = -1;
		return loadFileContent(host, port, path);
	}

	public static InputStream loadFileContent(String host, int port, String path) throws IOException
	{
		FTPClient ftpClient = new FTPClient();

		byte[] byteArray;
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream())
		{
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

			resultOk &= ftpClient.login("anonymous", "");
			LOG.info(ftpClient.getReplyString());
			resultOk &= ftpClient.retrieveFile(path, outputStream);
			LOG.info(ftpClient.getReplyString());
			resultOk &= ftpClient.logout();
			LOG.info(ftpClient.getReplyString());
			LOG.info("Success:" + resultOk);

			//
			byteArray = outputStream.toByteArray();
			return IOUtils.toBufferedInputStream(new ByteArrayInputStream(byteArray));
		}
		catch (Exception e)
		{
			return null;
		}
	}
}
