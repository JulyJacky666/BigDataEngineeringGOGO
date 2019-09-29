package HW1;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.BSON;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class part5 {
    private static MongoClient client = MongoClients.create();
    public static void main(String[] args) {
        System.out.println("Inserted :"+LoadDatAFromLogFile("datas/access.log"));

    }

    public static int LoadDatAFromLogFile(String path){
        Scanner datas;
        MongoCollection logCollection = client.getDatabase("INFO7250").getCollection("HW1_PART5_LOGFILE");

        List<Document> documents = new ArrayList<Document>();
        int counter = 0;
        try {
            datas = new Scanner(new File(path));
            while (datas.hasNextLine()) {
                counter++;
                Document document = new Document();
                String data = datas.nextLine();
                document.put("content", data);
                documents.add(document);

            }
            datas.close();
            logCollection.insertMany(documents);

        } catch (FileNotFoundException e) {
            counter = -1;
            e.printStackTrace();
        }

        return counter;


    }
}
