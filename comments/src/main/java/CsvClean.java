import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Omar Rivera
 * @version 1.0 11/8/21 15:34
 */
public class CsvClean {
    public static void main(String[] args) throws IOException {
        File file = new File(
                CsvClean.class.getClassLoader().getResource("attack_annotated_comments1000-1.csv").getFile()
        );
        FileInputStream inputStream = null;
        Scanner sc = null;

        CsvClean csvClean = new CsvClean();
        File csvOutputFile = new File("testout.csv");
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            try {
                inputStream = new FileInputStream(file);
                sc = new Scanner(inputStream, "UTF-8");
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    line = line.replace("NEWLINE_TOKEN", "");
                    line = line.replace("\\|", ">");
                    line = line.replaceAll("\\|", ">");
                    System.out.println(line);
                    Pattern pattern = Pattern.compile("\\s*(\"[^\"]*\"|[^,]*)\\s*");
                    Matcher matcher = pattern.matcher(line);
                    //String[] split = new String[matcher.groupCount()];
                    List<String> split = new ArrayList<>();
                    int flag = 1;
                    while(matcher.find()){
                        if(flag == 1) {
                            split.add(matcher.group(1));
                        }
                        flag = 1 - flag;
                    }
                    pw.println(csvClean.convertToCSV(split.toArray(new String[0])));
                }
                // note that Scanner suppresses exceptions
                if (sc.ioException() != null) {
                    throw sc.ioException();
                }
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (sc != null) {
                    sc.close();
                }
            }
        }
    }

    public String convertToCSV(String[] data) {
        return Stream.of(data)
                //.map(this::escapeSpecialCharacters)
                .collect(Collectors.joining("|"));
    }

    public String escapeSpecialCharacters(String data) {
        String escapedData = data.replaceAll("\\R", " ");
        if (data.contains(",") || data.contains("\"") || data.contains("'")) {
            data = data.replace("\"", "\"\"");
            escapedData = "\"" + data + "\"";
        }
        return escapedData;
    }
}
