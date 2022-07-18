/*
 *
 *  Copyright (c) 2022 [The original author]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package com.flipkart.varidhi.core.utils;


import com.flipkart.varidhi.config.EmailConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class EmailNotifier {
    private static final Logger logger = LoggerFactory.getLogger(EmailNotifier.class);

    private final EmailConfiguration emailConfiguration;
    private final String HOST_PROPERTY = "mail.smtp.host";

    public EmailNotifier(EmailConfiguration emailConfiguration) {
        this.emailConfiguration = emailConfiguration;
    }

    private void setToReceipts(Message msg, String[] recipientId) throws MessagingException {
        InternetAddress[] addresses = new InternetAddress[recipientId.length];
        for (int i = 0; i < recipientId.length; i++) {
            addresses[i] = new InternetAddress(recipientId[i]);
        }
        msg.setRecipients(Message.RecipientType.TO, addresses);

    }

    private Message sendMail(String header, String from, String to[], String host, String body) {
        Properties props = new Properties();
        props.put(HOST_PROPERTY, host);
        Session session = Session.getInstance(props);
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));
            setToReceipts(msg, to);
            msg.setSubject(isNotBlank(header) ? header : "Mail ");
            msg.setSentDate(new Date());
            msg.setContent(body, "text/html");
            return msg;
        } catch (AddressException ex) {
            logger.error("Unable to send email to " + Arrays.toString(to), ex);
        } catch (MessagingException ex) {
            logger.error("Unable to send email to " + Arrays.toString(to), ex);
        }
        return null;
    }


    public boolean newNotification(String body, String header, String recipients) {
        try {
            Message msg = sendMail(header, emailConfiguration.getFromAddress(), recipients.split(","), emailConfiguration.getSmtpHost(), body);
            Transport.send(msg);
            return true;
        } catch (MessagingException e) {
            logger.error("Unable to send email to " + recipients, e);
        }
        return false;
    }


}
