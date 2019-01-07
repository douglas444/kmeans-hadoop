import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;
import java.io.*;

public class Doc2Point {

    public static void main(String args[]) throws IOException {

      if (args.length == 0) {
        System.out.println("Invalid number of parameters");
        System.exit(1);
      }

      List<List<Integer>> wordCountPerDoc = new ArrayList<>();

      int wordNextIndex = 0;
      int docNextIndex = 0;

      Map<String, Integer> indexByWord = new HashMap<>();
      Map<String, Integer> indexByDoc = new HashMap<>();

      BufferedReader in = new BufferedReader(new FileReader(args[0]));

      String line;

      while((line = in.readLine()) != null) {

        line = line.replaceAll("\t", "@");
        String[] split = line.split("@");
        String docName = split[0];
        String word = split[1];
        int wordCount = Integer.parseInt(split[2]);

        if(indexByWord.get(word) == null) {
          indexByWord.put(word, wordNextIndex++);
          wordCountPerDoc.forEach(doc -> doc.add(0));
        }

        if (indexByDoc.get(docName) == null) {
          indexByDoc.put(docName, docNextIndex++);
          wordCountPerDoc.add(new ArrayList<Integer>(Collections.nCopies(wordNextIndex, 0)));
        }

        int wordIndex = indexByWord.get(word);
        int docIndex = indexByDoc.get(docName);

        wordCountPerDoc.get(docIndex).set(wordIndex, wordCount);

      }


      BufferedWriter writer = new BufferedWriter(new FileWriter("word_count_per_doc"));

      for (List<Integer> doc : wordCountPerDoc) {
        try {
            
          for (Integer wordCount : doc) {
              try {
                  writer.write(wordCount + " ");
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
          writer.write("\n");

        } catch (IOException e) {
          e.printStackTrace();
        }

      }

      writer.close();

  }
}
