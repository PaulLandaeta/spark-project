package com.alpha.generator.control;


import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;

import com.alpha.generator.entity.Channels;

@ApplicationScoped
public class MessageService {

    private static final String RESULT_CSV = "result.csv";
    private static final Logger LOG = LoggerFactory.getLogger(MessageService.class);    
    private List < String > terms = List.of("atheist",
        "queer",
        "gay",
        "transgender",
        "lesbian",
        "homosexual",
        "feminist",
        "black",
        "white",
        "heterosexual",
        "islam",
        "muslim",
        "bisexual");


    @Incoming(Channels.MESSAGE_IN)
    public void receive(String message) {

        message = normalizeMessage(message);
        String comment = comment(message);

        message = message + identity(comment);
        System.out.println("------------------\n");
        System.out.println(message);
        try {
            writeCsvFile(message);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }



    }

    private String normalizeMessage(String message) {
        String result = message.replace("Struct{rev_id=", "")
            .replace(",comment=", "|")
            .replace(",year=", "|")
            .replace(",logged_in=", "|")
            .replace(",ns=", "|")
            .replace(",sample=", "|")
            .replace(",split=train}", "|train")
            .replace(",split=dev}", "|dev")
            .replace(",split=test}", "|test");

        return result;
    }

    private String identity(String comment) {
        String result = "";

        for (String t: terms) {
            if (comment.contains(t)) {
                result = result + "|1";
            } else {
                result = result + "|0";
            }
        }

        return result;
    }

    private String comment(String message) {
        Pattern pattern = Pattern.compile("\\s*(\"[^\"]*\"|[^|]*)\\s*");
        Matcher matcher = pattern.matcher(message);
        String result = "";
        int flag = 1;
        int position = 3;
        while (matcher.find() && position > 0) {
            if (flag == 1) {
                result = matcher.group(1);
            }
            flag = 1 - flag;
            position--;
        }
        return result;
    }


    private void writeCsvFile(String line) throws IOException {
        File csvOutputFile = new File(RESULT_CSV);
        PrintWriter out = null;


        if (csvOutputFile.exists() && !csvOutputFile.isDirectory()) {
            out = new PrintWriter(new FileOutputStream(new File(RESULT_CSV), true));
        } else {
            out = new PrintWriter(RESULT_CSV);
        }

        out.append(line + "\n");
        out.close();

    }
}

