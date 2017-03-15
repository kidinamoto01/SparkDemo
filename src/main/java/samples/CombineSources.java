package samples;

import org.ansj.domain.Result;
import org.ansj.library.UserDefineLibrary;
import org.ansj.recognition.impl.FilterRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by b on 17/3/6.
 */


public class CombineSources {



    public static void main(String[] args) throws Exception {

        List<String> results = new ArrayList<>();
        Path  output= null;
        // Customize the dictionary
        UserDefineLibrary.removeWord("9.");//去掉标点符号
        UserDefineLibrary.insertWord("克拉克");
        FilterRecognition fitler = new FilterRecognition();
        fitler.insertStopNatures("null"); //过滤标点符号词性
        fitler.insertStopNatures("w"); //过滤标点符号词性
        fitler.insertStopNatures("en"); //过滤英文词性
        fitler.insertStopNatures("m"); //过滤标点符号词性
        //http://nlpchina.github.io/ansj_seg/content.html?nam
        //upper folder
        String srcFullDir = "/Users/b/Documents/andlinks/answer";
        File fullDir = new File(srcFullDir);

        List<File> subFolders = new ArrayList<>();
        for(File folder:fullDir.listFiles()){

            //iterate all the subfolders
            if(folder.isDirectory()){

                System.out.println("subfolder"+folder.toString());


                // Input files folder

               // String srcDic ="/Users/b/Documents/andlinks/answer/C3-Art";
                String srcDic =folder.toString();

                List<Path> textFiles = new ArrayList<>();

                File dir = new File(srcDic);
                for (File file : dir.listFiles()) {
                    if (file.getName().endsWith((".txt"))) {
                        textFiles.add(  Paths.get(file.getName()));
                    }
                }

                //type

                String type = "";

                // Output file
                output = Paths.get(srcFullDir+"/file.csv");

                // Charset for read and write
                Charset charset = StandardCharsets.UTF_8;

                // Join files (lines)
                for (Path name : textFiles) {



                    type = name.toString().substring(0,name.toString().indexOf('-')).substring(1);
                    String str = type+",";
                    System.out.println("Type "+type);

                   // results.add(type+" | ");
                    try {
                        Path path = Paths.get(srcDic, name.toString());

                        List<String> lines = Files.readAllLines(path, charset);

                        for(String line:lines){
                            if (!line.trim().isEmpty()) {

                                //System.out.println(line);
                                Result result = ToAnalysis.parse(line).recognition(fitler);


                                if (result.toString()!=""){
                                    //System.out.print(result.toStringWithOutNature(" "));

                                    str+=" "+ result.toStringWithOutNature(" ");
                                   // System.out.println("str:    "+str+".......");

                                    //results.add(result.toStringWithOutNature(" "));
                                }
                            }
                        }
                        //System.out.println(str);
                        results.add(str);



                    } catch (IOException e) {

                        System.out.println(e);
                    }

                }
        }

        }


        //create if not exist
        if (!Files.exists(output)){
            Files.createFile(output);
        }

        //write into the file
        Path write = Files.write(output, results, StandardCharsets.UTF_8, StandardOpenOption.APPEND);

    }
}
